// Copyright 2019 The Gaea Authors. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package executor

import (
	"context"
	"fmt"
	"github.com/XiaoMi/Gaea/common/constant"
	"github.com/XiaoMi/Gaea/core/plan"
	"github.com/XiaoMi/Gaea/core/router"
	"github.com/XiaoMi/Gaea/util/log"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/XiaoMi/Gaea/backend"
	"github.com/XiaoMi/Gaea/mysql"
	"github.com/XiaoMi/Gaea/parser"
	"github.com/XiaoMi/Gaea/parser/ast"
	"github.com/XiaoMi/Gaea/parser/format"
	"github.com/XiaoMi/Gaea/util"
	"github.com/XiaoMi/Gaea/util/hack"
)

const (
	GeneralLogVariable   = "general_log"
	InitClientConnStatus = mysql.ServerStatusAutocommit
)

// SessionExecutor is bound to a session, so requests are serializable
type SessionExecutor struct {
	Manager *Manager

	Namespace  string
	User       string
	Db         string
	ClientAddr string

	Status       uint16
	lastInsertID uint64

	collation        mysql.CollationID
	charset          string
	sessionVariables *mysql.SessionVariables

	txConns    map[string]backend.PooledConnect
	savepoints []string
	txLock     sync.Mutex

	stmtID uint32
	stmts  map[uint32]*Stmt //prepare相关,client端到proxy的stmt

	parser *parser.Parser
}

// GetNamespace return namespace in session
func (se *SessionExecutor) GetNamespace() *Namespace {
	return se.Manager.GetNamespace(se.Namespace)
}

// GetVariables return variables in session
func (se *SessionExecutor) GetVariables() *mysql.SessionVariables {
	return se.sessionVariables
}

func (se *SessionExecutor) setIntSessionVariable(name string, valueStr string) error {
	if strings.ToLower(valueStr) == mysql.KeywordDefault {
		se.sessionVariables.Delete(name)
		return nil
	}

	value, err := strconv.ParseInt(valueStr, 10, 64)
	if err != nil {
		return err
	}
	if err = se.sessionVariables.Set(name, value); err != nil {
		return err
	}
	return nil
}

func (se *SessionExecutor) setStringSessionVariable(name string, valueStr string) error {
	if strings.ToLower(valueStr) == mysql.KeywordDefault {
		se.sessionVariables.Delete(name)
		return nil
	}

	return se.sessionVariables.Set(name, valueStr)
}

func (se *SessionExecutor) setGeneralLogVariable(valueStr string) error {
	v, err := strconv.Atoi(valueStr)
	if err != nil {
		return constant.ErrInvalidArgument
	}
	atomic.StoreUint32(&ProcessGeneralLog, uint32(v))
	return nil
}

// GetLastInsertID return last_inert_id
func (se *SessionExecutor) GetLastInsertID() uint64 {
	return se.lastInsertID
}

// SetLastInsertID store last_insert_id
func (se *SessionExecutor) SetLastInsertID(id uint64) {
	se.lastInsertID = id
}

// GetStatus return session status
func (se *SessionExecutor) GetStatus() uint16 {
	return se.Status
}

// SetStatus store status
func (se *SessionExecutor) SetStatus(status uint16) {
	se.Status = status
}

// SetCollationID store collation id
func (se *SessionExecutor) SetCollationID(id mysql.CollationID) {
	se.collation = id
}

// SetNamespaceDefaultCollationID store default collation id
func (se *SessionExecutor) SetNamespaceDefaultCollationID() {
	se.collation = se.Manager.GetNamespace(se.Namespace).GetDefaultCollationID()
}

// GetCollationID return collation id
func (se *SessionExecutor) GetCollationID() mysql.CollationID {
	return se.collation
}

// SetCharset set session charset
func (se *SessionExecutor) SetCharset(charset string) {
	se.charset = charset
}

// SetNamespaceDefaultCharset set session default charset
func (se SessionExecutor) SetNamespaceDefaultCharset() {
	se.charset = se.Manager.GetNamespace(se.Namespace).GetDefaultCharset()
}

// GetCharset return charset
func (se *SessionExecutor) GetCharset() string {
	return se.charset
}

// SetDatabase set session database
func (se *SessionExecutor) SetDatabase(db string) {
	se.Db = db
}

// GetDatabase return database in session
func (se *SessionExecutor) GetDatabase() string {
	return se.Db
}

func (se *SessionExecutor) getBackendConns(sqls map[string]map[string][]string, fromSlave bool) (pcs map[string]backend.PooledConnect, err error) {
	pcs = make(map[string]backend.PooledConnect)
	for sliceName := range sqls {
		var pc backend.PooledConnect
		pc, err = se.getBackendConn(sliceName, fromSlave)
		if err != nil {
			return
		}
		pcs[sliceName] = pc
	}
	return
}

func (se *SessionExecutor) getBackendConn(sliceName string, fromSlave bool) (pc backend.PooledConnect, err error) {
	if !se.isInTransaction() {
		slice := se.GetNamespace().GetSlice(sliceName)
		return slice.GetConn(fromSlave, se.GetNamespace().GetUserProperty(se.User))
	}
	return se.getTransactionConn(sliceName)
}

func (se *SessionExecutor) getTransactionConn(sliceName string) (pc backend.PooledConnect, err error) {
	se.txLock.Lock()
	defer se.txLock.Unlock()

	var ok bool
	pc, ok = se.txConns[sliceName]

	if !ok {
		slice := se.GetNamespace().GetSlice(sliceName) // returns nil only when the conf is error (fatal) so panic is correct
		if pc, err = slice.GetMasterConn(); err != nil {
			return
		}

		if !se.isAutoCommit() {
			if err = pc.SetAutoCommit(0); err != nil {
				pc.Close()
				pc.Recycle()
				return
			}
		} else {
			if err = pc.Begin(); err != nil {
				pc.Close()
				pc.Recycle()
				return
			}
		}
		for _, savepoint := range se.savepoints {
			pc.Execute("savepoint "+savepoint, 0)
		}
		se.txConns[sliceName] = pc
	}

	return
}

func (se *SessionExecutor) recycleBackendConn(pc backend.PooledConnect, rollback bool) {
	if pc == nil {
		return
	}

	if se.isInTransaction() {
		return
	}

	if rollback {
		pc.Rollback()
	}

	pc.Recycle()
}

func (se *SessionExecutor) recycleBackendConns(pcs map[string]backend.PooledConnect, rollback bool) {
	if se.isInTransaction() {
		return
	}

	for _, pc := range pcs {
		if pc == nil {
			continue
		}
		if rollback {
			pc.Rollback()
		}
		pc.Recycle()
	}
}

func initBackendConn(pc backend.PooledConnect, phyDB string, charset string, collation mysql.CollationID, sessionVariables *mysql.SessionVariables) error {
	if err := pc.UseDB(phyDB); err != nil {
		return err
	}

	charsetChanged, err := pc.SetCharset(charset, collation)
	if err != nil {
		return err
	}

	variablesChanged, err := pc.SetSessionVariables(sessionVariables)
	if err != nil {
		return err
	}

	if charsetChanged || variablesChanged {
		if err = pc.WriteSetStatement(); err != nil {
			return err
		}
	}

	return nil
}

func (se *SessionExecutor) executeInMultiSlices(reqCtx *util.RequestContext, pcs map[string]backend.PooledConnect,
	sqls map[string]map[string][]string) ([]*mysql.Result, error) {

	parallel := len(pcs)
	if parallel != len(sqls) {
		log.Warn("Session executeInMultiSlices error, conns: %v, sqls: %v, error: %s", pcs, sqls, constant.ErrConnNotEqual.Error())
		return nil, constant.ErrConnNotEqual
	} else if parallel == 0 {
		return nil, constant.ErrNoPlan
	}

	var ctx = context.Background()
	var cancel context.CancelFunc
	maxExecuteTime := se.Manager.GetNamespace(se.Namespace).GetMaxExecuteTime()
	if maxExecuteTime > 0 {
		ctx, cancel = context.WithTimeout(context.Background(), time.Duration(maxExecuteTime)*time.Millisecond)
		defer cancel()
	}

	// Control go routine execution
	done := make(chan string, parallel)
	defer close(done)

	// This map is not thread safe.
	pcsUnCompleted := make(map[string]backend.PooledConnect, parallel)
	for sliceName, pc := range pcs {
		pcsUnCompleted[sliceName] = pc
	}

	resultCount := 0
	for _, sqlSlice := range sqls {
		for _, sqlDB := range sqlSlice {
			resultCount += len(sqlDB)
		}
	}
	rs := make([]interface{}, resultCount)
	f := func(reqCtx *util.RequestContext, rs []interface{}, i int, sliceName string, execSqls map[string][]string, pc backend.PooledConnect) {
		for db, sqls := range execSqls {
			err := initBackendConn(pc, db, se.GetCharset(), se.GetCollationID(), se.GetVariables())
			if err != nil {
				rs[i] = err
				break
			}
			for _, v := range sqls {
				startTime := time.Now()
				r, err := pc.Execute(v, se.Manager.GetNamespace(se.Namespace).GetMaxResultSize())
				se.Manager.RecordBackendSQLMetrics(reqCtx, se.Namespace, v, pc.GetAddr(), startTime, err)
				if err != nil {
					rs[i] = err
				} else {
					rs[i] = r
				}
				i++
			}
		}
		done <- sliceName
	}

	offset := 0
	for sliceName, pc := range pcs {
		s := sqls[sliceName] //map[string][]string
		go f(reqCtx, rs, offset, sliceName, s, pc)
		for _, sqlDB := range sqls[sliceName] {
			offset += len(sqlDB)
		}
	}

	for i := 0; i < parallel; i++ {
		select {
		case sliceName := <-done:
			delete(pcsUnCompleted, sliceName)
		case <-ctx.Done():
			for sliceName, pc := range pcsUnCompleted {
				connID := pc.GetConnectionID()
				dc, err := se.Manager.GetNamespace(se.Namespace).GetSlice(sliceName).GetDirectConn(pc.GetAddr())
				if err != nil {
					log.Warn("kill thread id: %d failed, get connection err: %v", connID, err.Error())
					continue
				}
				if _, err = dc.Execute(fmt.Sprintf("KILL QUERY %d", connID), 0); err != nil {
					log.Warn("kill thread id: %d failed, err: %v", connID, err.Error())
				}
				dc.Close()
			}
			for j := 0; j < len(pcsUnCompleted); j++ {
				<-done
			}
			return nil, fmt.Errorf("%v %dms", constant.ErrTimeLimitExceeded, maxExecuteTime)
		}
	}

	var err error
	r := make([]*mysql.Result, resultCount)
	for i, v := range rs {
		if e, ok := v.(error); ok {
			err = e
			break
		}
		if rs[i] != nil {
			r[i] = rs[i].(*mysql.Result)
		}
	}
	return r, err
}

const variableRestoreFlag = format.RestoreKeyWordLowercase | format.RestoreNameLowercase

// 获取SET语句中变量的字符串值, 去掉各种引号并转换为小写
func getVariableExprResult(v ast.ExprNode) string {
	s := &strings.Builder{}
	ctx := format.NewRestoreCtx(variableRestoreFlag, s)
	v.Restore(ctx)
	return strings.ToLower(s.String())
}

// canExecuteFromSlave master-slave routing
func canExecuteFromSlave(c *SessionExecutor, sql string) bool {
	result := router.CanExecuteFromSlave(sql)
	if !result {
		return false
	}
	return c.GetNamespace().IsRWSplit(c.User)
}

// 如果是只读用户, 且SQL是INSERT, UPDATE, DELETE, 则拒绝执行, 返回true
func isSQLNotAllowedByUser(c *SessionExecutor, stmtType int) bool {
	if c.GetNamespace().IsAllowWrite(c.User) {
		return false
	}

	return stmtType == parser.StmtDelete || stmtType == parser.StmtInsert || stmtType == parser.StmtUpdate
}

func modifyResultStatus(r *mysql.Result, cc *SessionExecutor) {
	r.Status = r.Status | cc.GetStatus()
}

func createShowDatabaseResult(dbs []string) *mysql.Result {
	r := new(mysql.Resultset)

	field := &mysql.Field{}
	field.Name = hack.Slice("Database")
	r.Fields = append(r.Fields, field)

	for _, db := range dbs {
		r.Values = append(r.Values, []interface{}{db})
	}

	result := &mysql.Result{
		AffectedRows: uint64(len(dbs)),
		Resultset:    r,
	}

	plan.GenerateSelectResultRowData(result)
	return result
}

func createShowGeneralLogResult() *mysql.Result {
	r := new(mysql.Resultset)

	field := &mysql.Field{}
	field.Name = hack.Slice(GeneralLogVariable)
	r.Fields = append(r.Fields, field)

	var value string
	if OpenProcessGeneralQueryLog() {
		value = "ON"
	} else {
		value = "OFF"
	}
	r.Values = append(r.Values, []interface{}{value})
	result := &mysql.Result{
		AffectedRows: 1,
		Resultset:    r,
	}

	plan.GenerateSelectResultRowData(result)
	return result
}

func getFromSlave(reqCtx *util.RequestContext) bool {
	slaveFlag := reqCtx.Get(util.FromSlave)
	if slaveFlag != nil && slaveFlag.(int) == 1 {
		return true
	}

	return false
}

func (se *SessionExecutor) isInTransaction() bool {
	return se.Status&mysql.ServerStatusInTrans > 0 ||
		!se.isAutoCommit()
}

func (se *SessionExecutor) isAutoCommit() bool {
	return se.Status&mysql.ServerStatusAutocommit > 0
}

func (se *SessionExecutor) handleBegin() error {
	se.txLock.Lock()
	defer se.txLock.Unlock()

	for _, co := range se.txConns {
		if err := co.Begin(); err != nil {
			return err
		}
	}
	se.Status |= mysql.ServerStatusInTrans
	se.savepoints = []string{}
	return nil
}

func (se *SessionExecutor) handleCommit() (err error) {
	if err := se.commit(); err != nil {
		return err
	}
	return nil

}

func (se *SessionExecutor) HandleRollback(stmt *ast.RollbackStmt) (err error) {
	if stmt == nil || stmt.Savepoint == "" {
		return se.Rollback()
	} else {
		return se.rollbackSavepoint(stmt.Savepoint)
	}
}

func (se *SessionExecutor) commit() (err error) {
	se.txLock.Lock()
	defer se.txLock.Unlock()

	se.Status &= ^mysql.ServerStatusInTrans

	for _, pc := range se.txConns {
		if e := pc.Commit(); e != nil {
			err = e
		}
		pc.Recycle()
	}

	se.txConns = make(map[string]backend.PooledConnect)
	se.savepoints = []string{}
	return
}

func (se *SessionExecutor) Rollback() (err error) {
	se.txLock.Lock()
	defer se.txLock.Unlock()
	se.Status &= ^mysql.ServerStatusInTrans
	for _, pc := range se.txConns {
		err = pc.Rollback()
		pc.Recycle()
	}
	se.txConns = make(map[string]backend.PooledConnect)
	se.savepoints = []string{}
	return
}

func (se *SessionExecutor) rollbackSavepoint(savepoint string) (err error) {
	se.txLock.Lock()
	defer se.txLock.Unlock()
	for _, pc := range se.txConns {
		_, err = pc.Execute("rollback to "+savepoint, 0)
	}
	if err == nil && se.isInTransaction() {
		if index := util.ArrayFindIndex(se.savepoints, savepoint); index > -1 {
			se.savepoints = se.savepoints[0:index]
		}
	}
	return
}

func (se *SessionExecutor) handleSavepoint(stmt *ast.SavepointStmt) (err error) {
	se.txLock.Lock()
	defer se.txLock.Unlock()
	if stmt.Release {
		for _, pc := range se.txConns {
			_, err = pc.Execute("release savepoint "+stmt.Savepoint, 0)
		}
		if err == nil && se.isInTransaction() {
			if index := util.ArrayFindIndex(se.savepoints, stmt.Savepoint); index > -1 {
				se.savepoints = se.savepoints[0 : index+1]
			}
		}
	} else {
		for _, pc := range se.txConns {
			_, err = pc.Execute("savepoint "+stmt.Savepoint, 0)
		}
		if err == nil && se.isInTransaction() {
			if util.ArrayFindIndex(se.savepoints, stmt.Savepoint) > -1 {
				se.savepoints = util.ArrayRemoveItem(se.savepoints, stmt.Savepoint)
			}
			se.savepoints = append(se.savepoints, stmt.Savepoint)
		}
	}
	return
}

// ExecuteSQL execute sql
func (se *SessionExecutor) ExecuteSQL(reqCtx *util.RequestContext, slice, db, sql string) (*mysql.Result, error) {
	phyDB, err := se.GetNamespace().GetDefaultPhyDB(db)
	if err != nil {
		return nil, err
	}

	sqls := make(map[string]map[string][]string)
	dbSQLs := make(map[string][]string)
	dbSQLs[phyDB] = []string{sql}
	sqls[slice] = dbSQLs

	pcs, err := se.getBackendConns(sqls, getFromSlave(reqCtx))
	defer se.recycleBackendConns(pcs, false)
	if err != nil {
		log.Warn("getUnShardConns failed: %v", err)
		return nil, err
	}

	rs, err := se.executeInMultiSlices(reqCtx, pcs, sqls)
	if err != nil {
		return nil, err
	}

	if len(rs) == 0 {
		return nil, mysql.NewError(mysql.ErrUnknown, "result is empty")
	}
	return rs[0], nil
}

// ExecuteSQLs len(sqls) must not be 0, or return error
func (se *SessionExecutor) ExecuteSQLs(reqCtx *util.RequestContext, sqls map[string]map[string][]string) ([]*mysql.Result, error) {
	if len(sqls) == 0 {
		return nil, fmt.Errorf("no sql to execute")
	}

	pcs, err := se.getBackendConns(sqls, getFromSlave(reqCtx))
	defer se.recycleBackendConns(pcs, false)
	if err != nil {
		log.Warn("getShardConns failed: %v", err)
		return nil, err
	}

	rs, err := se.executeInMultiSlices(reqCtx, pcs, sqls)
	if err != nil {
		return nil, err
	}
	return rs, nil
}

// Parse parse sql
func (se *SessionExecutor) Parse(sql string) (ast.StmtNode, error) {
	return se.parser.ParseOneStmt(sql, "", "")
}

// 处理query语句
func (se *SessionExecutor) HandleDMLQuery(sql string) (r *mysql.Result, err error) {
	defer func() {
		if e := recover(); e != nil {
			log.Warn("handle query command failed, error: %v, sql: %s", e, sql)

			if err, ok := e.(error); ok {
				const size = 4096
				buf := make([]byte, size)
				buf = buf[:runtime.Stack(buf, false)]

				log.Warn("handle query command catch panic error, sql: %s, error: %s, stack: %s",
					sql, err.Error(), string(buf))
			}

			err = constant.ErrInternalServer
			return
		}
	}()

	sql = strings.TrimRight(sql, ";") //删除sql语句最后的分号

	reqCtx := util.NewRequestContext()
	// check black sql
	ns := se.GetNamespace()
	if !ns.IsSQLAllowed(reqCtx, sql) {
		fingerprint := mysql.GetFingerprint(sql)
		log.Warn("catch black sql, sql: %s", sql)
		se.Manager.GetStatisticManager().RecordSQLForbidden(fingerprint, se.GetNamespace().GetName())
		err := mysql.NewError(mysql.ErrUnknown, "sql in blacklist")
		return nil, err
	}

	startTime := time.Now()
	stmtType := parser.Preview(sql)
	reqCtx.Set(util.StmtType, stmtType)

	r, err = se.doQuery(reqCtx, sql)
	se.Manager.RecordSessionSQLMetrics(reqCtx, se, sql, startTime, err)
	return r, err
}

func (se *SessionExecutor) doQuery(reqCtx *util.RequestContext, sql string) (*mysql.Result, error) {
	stmtType := reqCtx.Get("stmtType").(int)

	if isSQLNotAllowedByUser(se, stmtType) {
		return nil, fmt.Errorf("write DML is now allowed by read user")
	}

	if router.CanHandleWithoutPlan(stmtType) {
		return se.handleQueryWithoutPlan(reqCtx, sql)
	}

	db := se.Db

	p, err := se.getPlan(se.GetNamespace(), db, sql)
	if err != nil {
		return nil, fmt.Errorf("get plan error, db: %s, sql: %s, err: %v", db, sql, err)
	}

	if canExecuteFromSlave(se, sql) {
		reqCtx.Set(util.FromSlave, 1)
	}

	reqCtx.Set(util.DefaultSlice, se.GetNamespace().GetDefaultSlice())
	r, err := p.ExecuteIn(reqCtx, se)
	if err != nil {
		log.Warn("execute select: %s", err.Error())
		return nil, err
	}

	modifyResultStatus(r, se)

	return r, nil
}

// 处理逻辑较简单的SQL, 不走执行计划部分
func (se *SessionExecutor) handleQueryWithoutPlan(reqCtx *util.RequestContext, sql string) (*mysql.Result, error) {
	n, err := se.Parse(sql)
	if err != nil {
		return nil, fmt.Errorf("parse sql error, sql: %s, err: %v", sql, err)
	}

	switch stmt := n.(type) {
	case *ast.ShowStmt:
		return se.handleShow(reqCtx, sql, stmt)
	case *ast.SetStmt:
		return se.handleSet(reqCtx, sql, stmt)
	case *ast.BeginStmt:
		return nil, se.handleBegin()
	case *ast.CommitStmt:
		return nil, se.handleCommit()
	case *ast.RollbackStmt:
		return nil, se.HandleRollback(stmt)
	case *ast.SavepointStmt:
		return nil, se.handleSavepoint(stmt)
	case *ast.UseStmt:
		return nil, se.HandleUseDB(stmt.DBName)
	default:
		return nil, fmt.Errorf("cannot handle sql without plan, ns: %s, sql: %s", se.Namespace, sql)
	}
}

func (se *SessionExecutor) HandleUseDB(dbName string) error {
	if len(dbName) == 0 {
		return fmt.Errorf("must have database, the length of dbName is zero")
	}

	if se.GetNamespace().IsAllowedDB(dbName) {
		se.Db = dbName
		return nil
	}

	return mysql.NewDefaultError(mysql.ErrNoDB)
}

func (se *SessionExecutor) getPlan(ns *Namespace, db string, sql string) (plan.Plan, error) {
	n, err := se.Parse(sql)
	if err != nil {
		return nil, fmt.Errorf("parse sql error, sql: %s, err: %v", sql, err)
	}

	router := ns.GetRouter()
	seq := ns.GetSequences()
	phyDBs := ns.GetPhysicalDBs()
	p, err := plan.BuildPlan(n, phyDBs, db, sql, router, seq)
	if err != nil {
		return nil, fmt.Errorf("create select plan error: %v", err)
	}

	return p, nil
}

func NewSessionExecutor(manager *Manager) *SessionExecutor {

	return &SessionExecutor{
		sessionVariables: mysql.NewSessionVariables(),
		txConns:          make(map[string]backend.PooledConnect),
		stmts:            make(map[uint32]*Stmt),
		parser:           parser.New(),
		Status:           InitClientConnStatus,
		Manager:          manager,
	}
}

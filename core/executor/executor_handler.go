package executor

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"github.com/XiaoMi/Gaea/backend"
	"github.com/XiaoMi/Gaea/mysql"
	"github.com/XiaoMi/Gaea/parser/ast"
	"github.com/XiaoMi/Gaea/util"
	"github.com/XiaoMi/Gaea/util/log"
	"strings"
)

func (se *SessionExecutor) handleShow(reqCtx *util.RequestContext, sql string, stmt *ast.ShowStmt) (*mysql.Result, error) {
	switch stmt.Tp {
	case ast.ShowDatabases:
		dbs := se.GetNamespace().GetAllowedDBs()
		return createShowDatabaseResult(dbs), nil
	case ast.ShowVariables:
		if strings.Contains(sql, GeneralLogVariable) {
			return createShowGeneralLogResult(), nil
		}
		fallthrough
	default:
		r, err := se.ExecuteSQL(reqCtx, se.GetNamespace().GetDefaultSlice(), se.Db, sql)
		if err != nil {
			return nil, fmt.Errorf("execute sql error, sql: %s, err: %v", sql, err)
		}
		modifyResultStatus(r, se)
		return r, nil
	}
}

func (se *SessionExecutor) handleSet(reqCtx *util.RequestContext, sql string, stmt *ast.SetStmt) (*mysql.Result, error) {
	for _, v := range stmt.Variables {
		if err := se.handleSetVariable(v); err != nil {
			return nil, err
		}
	}

	return nil, nil
}

func (se *SessionExecutor) handleSetVariable(v *ast.VariableAssignment) error {
	if v.IsGlobal {
		return fmt.Errorf("does not support set variable in global scope")
	}
	name := strings.ToLower(v.Name)
	switch name {
	case "character_set_results", "character_set_client", "character_set_connection":
		charset := getVariableExprResult(v.Value)
		if charset == "null" { // character_set_results允许设置成null, character_set_client和character_set_connection不允许
			return nil
		}
		if charset == mysql.KeywordDefault {
			se.charset = se.GetNamespace().GetDefaultCharset()
			se.collation = se.GetNamespace().GetDefaultCollationID()
			return nil
		}
		cid, ok := mysql.CharsetIds[charset]
		if !ok {
			return mysql.NewDefaultError(mysql.ErrUnknownCharacterSet, charset)
		}
		se.charset = charset
		se.collation = cid
		return nil
	case "autocommit":
		value := getVariableExprResult(v.Value)
		if value == mysql.KeywordDefault || value == "on" || value == "1" {
			return se.handleSetAutoCommit(true) // default set autocommit = 1
		} else if value == "off" || value == "0" {
			return se.handleSetAutoCommit(false)
		} else {
			return mysql.NewDefaultError(mysql.ErrWrongValueForVar, name, value)
		}
	case "setnames": // SetNAMES represents SET NAMES 'xxx' COLLATE 'xxx'
		charset := getVariableExprResult(v.Value)
		if charset == mysql.KeywordDefault {
			charset = se.GetNamespace().GetDefaultCharset()
		}

		var collationID mysql.CollationID
		// if SET NAMES 'xxx' COLLATE DEFAULT, the parser treats it like SET NAMES 'xxx', and the ExtendValue is nil
		if v.ExtendValue != nil {
			collationName := getVariableExprResult(v.ExtendValue)
			cid, ok := mysql.CollationNames[collationName]
			if !ok {
				return mysql.NewDefaultError(mysql.ErrUnknownCharacterSet, charset)
			}
			toCharset, ok := mysql.CollationNameToCharset[collationName]
			if !ok {
				return mysql.NewDefaultError(mysql.ErrUnknownCharacterSet, charset)
			}
			if toCharset != charset { // collation与charset不匹配
				return mysql.NewDefaultError(mysql.ErrUnknownCharacterSet, charset)
			}
			collationID = cid
		} else {
			// if only set charset but not set collation, the collation is set to charset default collation implicitly.
			cid, ok := mysql.CharsetIds[charset]
			if !ok {
				return mysql.NewDefaultError(mysql.ErrUnknownCharacterSet, charset)
			}
			collationID = cid
		}

		se.charset = charset
		se.collation = collationID
		return nil
	case "sql_mode":
		sqlMode := getVariableExprResult(v.Value)
		return se.setStringSessionVariable(mysql.SQLModeStr, sqlMode)
	case "sql_safe_updates":
		value := getVariableExprResult(v.Value)
		onOffValue, err := util.GetOnOffVariable(value)
		if err != nil {
			return mysql.NewDefaultError(mysql.ErrWrongValueForVar, name, value)
		}
		return se.setIntSessionVariable(mysql.SQLSafeUpdates, onOffValue)
	case "time_zone":
		value := getVariableExprResult(v.Value)
		return se.setStringSessionVariable(mysql.TimeZone, value)
	case "max_allowed_packet":
		return mysql.NewDefaultError(mysql.ErrVariableIsReadonly, "SESSION", mysql.MaxAllowedPacket, "GLOBAL")

		// do nothing
	case "wait_timeout", "interactive_timeout", "net_write_timeout", "net_read_timeout":
		return nil
	case "sql_select_limit":
		return nil
		// unsupported
	case "transaction":
		return fmt.Errorf("does not support set transaction in gaea")
	case GeneralLogVariable:
		value := getVariableExprResult(v.Value)
		onOffValue, err := util.GetOnOffVariable(value)
		if err != nil {
			return mysql.NewDefaultError(mysql.ErrWrongValueForVar, name, value)
		}
		return se.setGeneralLogVariable(onOffValue)
	default:
		return nil
	}
}

func (se *SessionExecutor) handleSetAutoCommit(autocommit bool) (err error) {
	se.txLock.Lock()
	defer se.txLock.Unlock()

	if autocommit {
		se.Status |= mysql.ServerStatusAutocommit
		if se.Status&mysql.ServerStatusInTrans > 0 {
			se.Status &= ^mysql.ServerStatusInTrans
		}
		for _, pc := range se.txConns {
			if e := pc.SetAutoCommit(1); e != nil {
				err = fmt.Errorf("set autocommit error, %v", e)
			}
			pc.Recycle()
		}
		se.txConns = make(map[string]backend.PooledConnect)
		return
	}

	se.Status &= ^mysql.ServerStatusAutocommit
	return
}

func (se *SessionExecutor) HandleStmtPrepare(sql string) (*Stmt, error) {
	log.Debug("namespace: %s use prepare, sql: %s", se.GetNamespace().GetName(), sql)

	stmt := new(Stmt)

	sql = strings.TrimRight(sql, ";")
	stmt.sql = sql

	paramCount, offsets, err := calcParams(stmt.sql)
	if err != nil {
		log.Warn("prepare calc params failed, namespace: %s, sql: %s", se.GetNamespace().GetName(), sql)
		return nil, err
	}

	stmt.ParamCount = paramCount
	stmt.offsets = offsets
	stmt.Id = se.stmtID
	stmt.ColumnCount = 0
	se.stmtID++

	stmt.ResetParams()
	se.stmts[stmt.Id] = stmt

	return stmt, nil
}

func (se *SessionExecutor) HandleStmtClose(data []byte) error {
	if len(data) < 4 {
		return nil
	}

	id := binary.LittleEndian.Uint32(data[0:4])

	delete(se.stmts, id)

	return nil
}

func (se *SessionExecutor) HandleFieldList(data []byte) ([]*mysql.Field, error) {
	index := bytes.IndexByte(data, 0x00)
	table := string(data[0:index])
	wildcard := string(data[index+1:])

	sliceName := se.GetNamespace().GetRouter().GetRule(se.GetDatabase(), table).GetSlice(0)

	pc, err := se.getBackendConn(sliceName, se.GetNamespace().IsRWSplit(se.User))
	if err != nil {
		return nil, err
	}
	defer se.recycleBackendConn(pc, false)

	phyDB, err := se.GetNamespace().GetDefaultPhyDB(se.GetDatabase())
	if err != nil {
		return nil, err
	}

	if err = initBackendConn(pc, phyDB, se.GetCharset(), se.GetCollationID(), se.GetVariables()); err != nil {
		return nil, err
	}

	fs, err := pc.FieldList(table, wildcard)
	if err != nil {
		return nil, err
	}

	return fs, nil
}

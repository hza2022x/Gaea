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
	"github.com/XiaoMi/Gaea/common/constant"
	"github.com/XiaoMi/Gaea/util/log"
	"strings"
	"time"

	"github.com/XiaoMi/Gaea/models"
	"github.com/XiaoMi/Gaea/mysql"
	"github.com/XiaoMi/Gaea/parser"
	"github.com/XiaoMi/Gaea/util"
	"github.com/XiaoMi/Gaea/util/sync2"
)

// Manager contains namespace manager and user manager
type Manager struct {
	reloadPrepared sync2.AtomicBool
	switchIndex    util.BoolIndex
	namespaces     [2]*NamespaceManager
	users          [2]*UserManager
	statistics     *StatisticManager
}

// NewManager return empty Manager
func NewManager() *Manager {
	return &Manager{}
}

// Close close manager
func (m *Manager) Close() {
	current, _, _ := m.switchIndex.Get()

	namespaces := m.namespaces[current].namespaces
	for _, ns := range namespaces {
		ns.Close(false)
	}

	m.statistics.Close()
}

// ReloadNamespacePrepare prepare commit
func (m *Manager) ReloadNamespacePrepare(ns *models.Namespace) error {
	name := ns.Name
	current, other, _ := m.switchIndex.Get()

	// reload namespace prepare
	currentNamespaceManager := m.namespaces[current]
	newNamespaceManager := ShallowCopyNamespaceManager(currentNamespaceManager)
	if err := newNamespaceManager.RebuildNamespace(ns); err != nil {
		log.Warn("prepare config of namespace: %s failed, err: %v", name, err)
		return err
	}
	m.namespaces[other] = newNamespaceManager

	// reload user prepare
	currentUserManager := m.users[current]
	newUserManager := CloneUserManager(currentUserManager)
	newUserManager.RebuildNamespaceUsers(ns)
	m.users[other] = newUserManager
	m.reloadPrepared.Set(true)

	return nil
}

// ReloadNamespaceCommit commit config
func (m *Manager) ReloadNamespaceCommit(name string) error {
	if !m.reloadPrepared.CompareAndSwap(true, false) {
		err := constant.ErrNamespaceNotPrepared
		log.Warn("commit namespace error, namespace: %s, err: %v", name, err)
		return err
	}

	current, _, index := m.switchIndex.Get()

	currentNamespace := m.namespaces[current].GetNamespace(name)
	if currentNamespace != nil {
		go currentNamespace.Close(true)
	}

	m.switchIndex.Set(!index)

	return nil
}

// DeleteNamespace delete namespace
func (m *Manager) DeleteNamespace(name string) error {
	current, other, index := m.switchIndex.Get()

	// idempotent delete
	currentNamespace := m.namespaces[current].GetNamespace(name)
	if currentNamespace == nil {
		return nil
	}

	// delete namespace of other
	currentNamespaceManager := m.namespaces[current]
	newNamespaceManager := ShallowCopyNamespaceManager(currentNamespaceManager)
	newNamespaceManager.DeleteNamespace(name)
	m.namespaces[other] = newNamespaceManager

	// delete users of other
	currentUserManager := m.users[current]
	newUserManager := CloneUserManager(currentUserManager)
	newUserManager.ClearNamespaceUsers(name)
	m.users[other] = newUserManager

	// switch namespace manager
	m.switchIndex.Set(!index)

	// delay recycle resources of current
	go currentNamespace.Close(true)

	return nil
}

// GetNamespace return specific namespace
func (m *Manager) GetNamespace(name string) *Namespace {
	current, _, _ := m.switchIndex.Get()
	return m.namespaces[current].GetNamespace(name)
}

// CheckUser check if user in users
func (m *Manager) CheckUser(user string) bool {
	current, _, _ := m.switchIndex.Get()
	return m.users[current].CheckUser(user)
}

// CheckPassword check if right password with specific user
func (m *Manager) CheckPassword(user string, salt, auth []byte) (bool, string) {
	current, _, _ := m.switchIndex.Get()
	return m.users[current].CheckPassword(user, salt, auth)
}

// CheckPassword check if right password with specific user
func (m *Manager) CheckSha2Password(user string, salt, auth []byte) (bool, string) {
	current, _, _ := m.switchIndex.Get()
	return m.users[current].CheckSha2Password(user, salt, auth)
}

// GetStatisticManager return status to record status
func (m *Manager) GetStatisticManager() *StatisticManager {
	return m.statistics
}

// GetNamespaceByUser return namespace by user
func (m *Manager) GetNamespaceByUser(userName, password string) string {
	current, _, _ := m.switchIndex.Get()
	return m.users[current].GetNamespaceByUser(userName, password)
}

// ConfigFingerprint return config fingerprint
func (m *Manager) ConfigFingerprint() string {
	current, _, _ := m.switchIndex.Get()
	return m.namespaces[current].ConfigFingerprint()
}

// RecordSessionSQLMetrics record session SQL metrics, like response time, error
func (m *Manager) RecordSessionSQLMetrics(reqCtx *util.RequestContext, se *SessionExecutor, sql string, startTime time.Time, err error) {
	trimmedSql := strings.ReplaceAll(sql, "\n", " ")
	namespace := se.Namespace
	ns := m.GetNamespace(namespace)
	if ns == nil {
		log.Warn("record session SQL metrics error, namespace: %s, sql: %s, err: %s", namespace, trimmedSql, "namespace not found")
		return
	}

	var operation string
	if stmtType, ok := reqCtx.Get(util.StmtType).(int); ok {
		operation = parser.StmtType(stmtType)
	} else {
		fingerprint := mysql.GetFingerprint(sql)
		operation = mysql.GetFingerprintOperation(fingerprint)
	}

	// record sql timing
	m.statistics.recordSessionSQLTiming(namespace, operation, startTime)

	// record slow sql
	duration := time.Since(startTime).Nanoseconds() / int64(time.Millisecond)
	if duration > ns.getSessionSlowSQLTime() || ns.getSessionSlowSQLTime() == 0 {
		log.Warn("session slow SQL, namespace: %s, sql: %s, cost: %d ms", namespace, trimmedSql, duration)
		fingerprint := mysql.GetFingerprint(sql)
		md5 := mysql.GetMd5(fingerprint)
		ns.SetSlowSQLFingerprint(md5, fingerprint)
		m.statistics.recordSessionSlowSQLFingerprint(namespace, md5)
	}

	// record error sql
	if err != nil {
		log.Warn("session error SQL, namespace: %s, sql: %s, cost: %d ms, err: %v", namespace, trimmedSql, duration, err)
		fingerprint := mysql.GetFingerprint(sql)
		md5 := mysql.GetMd5(fingerprint)
		ns.SetErrorSQLFingerprint(md5, fingerprint)
		m.statistics.recordSessionErrorSQLFingerprint(namespace, operation, md5)
	}

	if OpenProcessGeneralQueryLog() && ns.openGeneralLog {
		m.statistics.generalLogger.Notice("client: %s, namespace: %s, db: %s, user: %s, cmd: %s, sql: %s, cost: %d ms, succ: %t",
			se.ClientAddr, namespace, se.Db, se.User, operation, trimmedSql, duration, err == nil)
	}
}

// RecordBackendSQLMetrics record backend SQL metrics, like response time, error
func (m *Manager) RecordBackendSQLMetrics(reqCtx *util.RequestContext, namespace string, sql, backendAddr string, startTime time.Time, err error) {
	trimmedSql := strings.ReplaceAll(sql, "\n", " ")
	ns := m.GetNamespace(namespace)
	if ns == nil {
		log.Warn("record backend SQL metrics error, namespace: %s, backend addr: %s, sql: %s, err: %s", namespace, backendAddr, trimmedSql, "namespace not found")
		return
	}

	var operation string
	if stmtType, ok := reqCtx.Get(util.StmtType).(int); ok {
		operation = parser.StmtType(stmtType)
	} else {
		fingerprint := mysql.GetFingerprint(sql)
		operation = mysql.GetFingerprintOperation(fingerprint)
	}

	// record sql timing
	m.statistics.recordBackendSQLTiming(namespace, operation, startTime)

	// record slow sql
	duration := time.Since(startTime).Nanoseconds() / int64(time.Millisecond)
	if m.statistics.isBackendSlowSQL(startTime) {
		log.Warn("backend slow SQL, namespace: %s, addr: %s, sql: %s, cost: %d ms", namespace, backendAddr, trimmedSql, duration)
		fingerprint := mysql.GetFingerprint(sql)
		md5 := mysql.GetMd5(fingerprint)
		ns.SetBackendSlowSQLFingerprint(md5, fingerprint)
		m.statistics.recordBackendSlowSQLFingerprint(namespace, md5)
	}

	// record error sql
	if err != nil {
		log.Warn("backend error SQL, namespace: %s, addr: %s, sql: %s, cost %d ms, err: %v", namespace, backendAddr, trimmedSql, duration, err)
		fingerprint := mysql.GetFingerprint(sql)
		md5 := mysql.GetMd5(fingerprint)
		ns.SetBackendErrorSQLFingerprint(md5, fingerprint)
		m.statistics.recordBackendErrorSQLFingerprint(namespace, operation, md5)
	}
}

func (m *Manager) startPoolMetricsTask(interval int) {
	if interval <= 0 {
		interval = 10
	}

	go func() {
		t := time.NewTicker(time.Duration(interval) * time.Second)
		for {
			select {
			case <-m.GetStatisticManager().closeChan:
				return
			case <-t.C:
				current, _, _ := m.switchIndex.Get()
				for nameSpaceName, _ := range m.namespaces[current].namespaces {
					m.recordBackendConnectPoolMetrics(nameSpaceName)
				}
			}
		}
	}()
}

func (m *Manager) recordBackendConnectPoolMetrics(namespace string) {
	ns := m.GetNamespace(namespace)
	if ns == nil {
		log.Warn("record backend connect pool metrics err, namespace: %s", namespace)
		return
	}

	for sliceName, slice := range ns.slices {
		m.statistics.recordPoolInuseCount(namespace, sliceName, slice.Master.Addr(), slice.Master.InUse())
		m.statistics.recordPoolIdleCount(namespace, sliceName, slice.Master.Addr(), slice.Master.Available())
		m.statistics.recordPoolWaitCount(namespace, sliceName, slice.Master.Addr(), slice.Master.WaitCount())
		for _, slave := range slice.Slave {
			m.statistics.recordPoolInuseCount(namespace, sliceName, slave.Addr(), slave.InUse())
			m.statistics.recordPoolIdleCount(namespace, sliceName, slave.Addr(), slave.Available())
			m.statistics.recordPoolWaitCount(namespace, sliceName, slave.Addr(), slave.WaitCount())
		}
		for _, statisticSlave := range slice.StatisticSlave {
			m.statistics.recordPoolInuseCount(namespace, sliceName, statisticSlave.Addr(), statisticSlave.InUse())
			m.statistics.recordPoolIdleCount(namespace, sliceName, statisticSlave.Addr(), statisticSlave.Available())
			m.statistics.recordPoolWaitCount(namespace, sliceName, statisticSlave.Addr(), statisticSlave.WaitCount())
		}
	}
}

func getUserKey(username, password string) string {
	return username + ":" + password
}

func getUserAndPasswordFromKey(key string) (username string, password string) {
	strs := strings.Split(key, ":")
	return strs[0], strs[1]
}

const (
	statsLabelCluster       = "Cluster"
	statsLabelOperation     = "Operation"
	statsLabelNamespace     = "Namespace"
	statsLabelFingerprint   = "Fingerprint"
	statsLabelFlowDirection = "Flowdirection"
	statsLabelSlice         = "Slice"
	statsLabelIPAddr        = "IPAddr"
)

type ProxyStatsConfig struct {
	Service      string
	StatsEnabled bool
}

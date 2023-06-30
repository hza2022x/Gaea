package executor

import (
	"github.com/XiaoMi/Gaea/log"
	"github.com/XiaoMi/Gaea/models"
	"github.com/XiaoMi/Gaea/mysql"
	"github.com/XiaoMi/Gaea/stats"
	"github.com/XiaoMi/Gaea/stats/prometheus"
	"net/http"
	"time"
)

// StatisticManager statistics manager
type StatisticManager struct {
	manager     *Manager
	clusterName string

	statsType     string // 监控后端类型
	handlers      map[string]http.Handler
	generalLogger log.Logger

	sqlTimings                *stats.MultiTimings            // SQL耗时统计
	sqlFingerprintSlowCounts  *stats.CountersWithMultiLabels // 慢SQL指纹数量统计
	sqlErrorCounts            *stats.CountersWithMultiLabels // SQL错误数统计
	sqlFingerprintErrorCounts *stats.CountersWithMultiLabels // SQL指纹错误数统计
	sqlForbidenCounts         *stats.CountersWithMultiLabels // SQL黑名单请求统计
	flowCounts                *stats.CountersWithMultiLabels // 业务流量统计
	sessionCounts             *stats.GaugesWithMultiLabels   // 前端会话数统计

	backendSQLTimings                *stats.MultiTimings            // 后端SQL耗时统计
	backendSQLFingerprintSlowCounts  *stats.CountersWithMultiLabels // 后端慢SQL指纹数量统计
	backendSQLErrorCounts            *stats.CountersWithMultiLabels // 后端SQL错误数统计
	backendSQLFingerprintErrorCounts *stats.CountersWithMultiLabels // 后端SQL指纹错误数统计
	backendConnectPoolIdleCounts     *stats.GaugesWithMultiLabels   //后端空闲连接数统计
	backendConnectPoolInUseCounts    *stats.GaugesWithMultiLabels   //后端正在使用连接数统计
	backendConnectPoolWaitCounts     *stats.GaugesWithMultiLabels   //后端等待队列统计

	slowSQLTime int64
	closeChan   chan bool
}

// NewStatisticManager return empty StatisticManager
func NewStatisticManager() *StatisticManager {
	return &StatisticManager{}
}

// Close close stats
func (s *StatisticManager) Close() {
	close(s.closeChan)
}

// GetHandlers return specific handler of stats
func (s *StatisticManager) GetHandlers() map[string]http.Handler {
	return s.handlers
}

func (s *StatisticManager) initBackend(cfg *ProxyStatsConfig) error {
	prometheus.Init(cfg.Service)
	s.handlers = prometheus.GetHandlers()
	return nil
}

// clear data to prevent
func (s *StatisticManager) startClearTask() {
	go func() {
		t := time.NewTicker(time.Hour)
		for {
			select {
			case <-s.closeChan:
				return
			case <-t.C:
				s.clearLargeCounters()
			}
		}
	}()
}

func (s *StatisticManager) clearLargeCounters() {
	s.sqlErrorCounts.ResetAll()
	s.sqlFingerprintSlowCounts.ResetAll()
	s.sqlFingerprintErrorCounts.ResetAll()

	s.backendSQLErrorCounts.ResetAll()
	s.backendSQLFingerprintSlowCounts.ResetAll()
	s.backendSQLFingerprintErrorCounts.ResetAll()
}

func (s *StatisticManager) recordSessionSlowSQLFingerprint(namespace string, md5 string) {
	fingerprintStatsKey := []string{s.clusterName, namespace, md5}
	s.sqlFingerprintSlowCounts.Add(fingerprintStatsKey, 1)
}

func (s *StatisticManager) recordSessionErrorSQLFingerprint(namespace string, operation string, md5 string) {
	fingerprintStatsKey := []string{s.clusterName, namespace, md5}
	operationStatsKey := []string{s.clusterName, namespace, operation}
	s.sqlErrorCounts.Add(operationStatsKey, 1)
	s.sqlFingerprintErrorCounts.Add(fingerprintStatsKey, 1)
}

func (s *StatisticManager) recordSessionSQLTiming(namespace string, operation string, startTime time.Time) {
	operationStatsKey := []string{s.clusterName, namespace, operation}
	s.sqlTimings.Record(operationStatsKey, startTime)
}

// millisecond duration
func (s *StatisticManager) isBackendSlowSQL(startTime time.Time) bool {
	duration := time.Since(startTime).Nanoseconds() / int64(time.Millisecond)
	return duration > s.slowSQLTime || s.slowSQLTime == 0
}

func (s *StatisticManager) recordBackendSlowSQLFingerprint(namespace string, md5 string) {
	fingerprintStatsKey := []string{s.clusterName, namespace, md5}
	s.backendSQLFingerprintSlowCounts.Add(fingerprintStatsKey, 1)
}

func (s *StatisticManager) recordBackendErrorSQLFingerprint(namespace string, operation string, md5 string) {
	fingerprintStatsKey := []string{s.clusterName, namespace, md5}
	operationStatsKey := []string{s.clusterName, namespace, operation}
	s.backendSQLErrorCounts.Add(operationStatsKey, 1)
	s.backendSQLFingerprintErrorCounts.Add(fingerprintStatsKey, 1)
}

func (s *StatisticManager) recordBackendSQLTiming(namespace string, operation string, startTime time.Time) {
	operationStatsKey := []string{s.clusterName, namespace, operation}
	s.backendSQLTimings.Record(operationStatsKey, startTime)
}

// RecordSQLForbidden record forbidden sql
func (s *StatisticManager) RecordSQLForbidden(fingerprint, namespace string) {
	md5 := mysql.GetMd5(fingerprint)
	s.sqlForbidenCounts.Add([]string{s.clusterName, namespace, md5}, 1)
}

// IncrSessionCount incr session count
func (s *StatisticManager) IncrSessionCount(namespace string) {
	statsKey := []string{s.clusterName, namespace}
	s.sessionCounts.Add(statsKey, 1)
}

// DescSessionCount decr session count
func (s *StatisticManager) DescSessionCount(namespace string) {
	statsKey := []string{s.clusterName, namespace}
	s.sessionCounts.Add(statsKey, -1)
}

// AddReadFlowCount add read flow count
func (s *StatisticManager) AddReadFlowCount(namespace string, byteCount int) {
	statsKey := []string{s.clusterName, namespace, "read"}
	s.flowCounts.Add(statsKey, int64(byteCount))
}

// AddWriteFlowCount add write flow count
func (s *StatisticManager) AddWriteFlowCount(namespace string, byteCount int) {
	statsKey := []string{s.clusterName, namespace, "write"}
	s.flowCounts.Add(statsKey, int64(byteCount))
}

// record idle connect count
func (s *StatisticManager) recordConnectPoolIdleCount(namespace string, slice string, addr string, count int64) {
	statsKey := []string{s.clusterName, namespace, slice, addr}
	s.backendConnectPoolIdleCounts.Set(statsKey, count)
}

// record in-use connect count
func (s *StatisticManager) recordConnectPoolInuseCount(namespace string, slice string, addr string, count int64) {
	statsKey := []string{s.clusterName, namespace, slice, addr}
	s.backendConnectPoolInUseCounts.Set(statsKey, count)
}

// record wait queue length
func (s *StatisticManager) recordConnectPoolWaitCount(namespace string, slice string, addr string, count int64) {
	statsKey := []string{s.clusterName, namespace, slice, addr}
	s.backendConnectPoolWaitCounts.Set(statsKey, count)
}

// Init init StatisticManager
func (s *StatisticManager) Init(cfg *models.Proxy) error {
	s.closeChan = make(chan bool, 0)
	s.handlers = make(map[string]http.Handler)
	s.slowSQLTime = cfg.SlowSQLTime
	statsCfg, err := ParseProxyStatsConfig(cfg)
	if err != nil {
		return err
	}

	if err := s.initBackend(statsCfg); err != nil {
		return err
	}

	s.sqlTimings = stats.NewMultiTimings("SqlTimings",
		"sql sqlTimings", []string{statsLabelCluster, statsLabelNamespace, statsLabelOperation})
	s.sqlFingerprintSlowCounts = stats.NewCountersWithMultiLabels("SqlFingerprintSlowCounts",
		"sql fingerprint slow counts", []string{statsLabelCluster, statsLabelNamespace, statsLabelFingerprint})
	s.sqlErrorCounts = stats.NewCountersWithMultiLabels("SqlErrorCounts",
		"sql error counts per error type", []string{statsLabelCluster, statsLabelNamespace, statsLabelOperation})
	s.sqlFingerprintErrorCounts = stats.NewCountersWithMultiLabels("SqlFingerprintErrorCounts",
		"sql fingerprint error counts", []string{statsLabelCluster, statsLabelNamespace, statsLabelFingerprint})
	s.sqlForbidenCounts = stats.NewCountersWithMultiLabels("SqlForbiddenCounts",
		"sql error counts per error type", []string{statsLabelCluster, statsLabelNamespace, statsLabelFingerprint})
	s.flowCounts = stats.NewCountersWithMultiLabels("FlowCounts",
		"flow counts", []string{statsLabelCluster, statsLabelNamespace, statsLabelFlowDirection})
	s.sessionCounts = stats.NewGaugesWithMultiLabels("SessionCounts",
		"session counts", []string{statsLabelCluster, statsLabelNamespace})

	s.backendSQLTimings = stats.NewMultiTimings("BackendSqlTimings",
		"backend sql sqlTimings", []string{statsLabelCluster, statsLabelNamespace, statsLabelOperation})
	s.backendSQLFingerprintSlowCounts = stats.NewCountersWithMultiLabels("BackendSqlFingerprintSlowCounts",
		"backend sql fingerprint slow counts", []string{statsLabelCluster, statsLabelNamespace, statsLabelFingerprint})
	s.backendSQLErrorCounts = stats.NewCountersWithMultiLabels("BackendSqlErrorCounts",
		"backend sql error counts per error type", []string{statsLabelCluster, statsLabelNamespace, statsLabelOperation})
	s.backendSQLFingerprintErrorCounts = stats.NewCountersWithMultiLabels("BackendSqlFingerprintErrorCounts",
		"backend sql fingerprint error counts", []string{statsLabelCluster, statsLabelNamespace, statsLabelFingerprint})
	s.backendConnectPoolIdleCounts = stats.NewGaugesWithMultiLabels("backendConnectPoolIdleCounts",
		"backend idle connect counts", []string{statsLabelCluster, statsLabelNamespace, statsLabelSlice, statsLabelIPAddr})
	s.backendConnectPoolInUseCounts = stats.NewGaugesWithMultiLabels("backendConnectPoolInUseCounts",
		"backend in-use connect counts", []string{statsLabelCluster, statsLabelNamespace, statsLabelSlice, statsLabelIPAddr})
	s.backendConnectPoolWaitCounts = stats.NewGaugesWithMultiLabels("backendConnectPoolWaitCounts",
		"backend wait connect counts", []string{statsLabelCluster, statsLabelNamespace, statsLabelSlice, statsLabelIPAddr})

	s.startClearTask()
	return nil
}

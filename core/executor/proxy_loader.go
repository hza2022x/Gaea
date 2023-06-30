package executor

import (
	"github.com/XiaoMi/Gaea/log"
	"github.com/XiaoMi/Gaea/log/xlog"
	"github.com/XiaoMi/Gaea/models"
	"github.com/XiaoMi/Gaea/stats"
	"net/http"
	"strconv"
	"sync"
)

// LoadAndCreateManager load namespace config, and create manager
func LoadAndCreateManager(cfg *models.Proxy) (*Manager, error) {
	namespaceConfigs, err := loadAllNamespace(cfg)
	if err != nil {
		log.Warn("init namespace manager failed, %v", err)
		return nil, err

	}

	mgr, err := CreateManager(cfg, namespaceConfigs)
	if err != nil {
		log.Warn("create manager error: %v", err)
		return nil, err
	}
	//globalManager = mgr
	return mgr, nil
}

func loadAllNamespace(cfg *models.Proxy) (map[string]*models.Namespace, error) {
	// get names of all namespace
	root := cfg.CoordinatorRoot
	if cfg.ConfigType == models.ConfigFile {
		root = cfg.FileConfigPath
	}

	client := models.NewClient(cfg.ConfigType, cfg.CoordinatorAddr, cfg.UserName, cfg.Password, root)
	store := models.NewStore(client)
	defer store.Close()
	var err error
	var names []string
	names, err = store.ListNamespace()
	if err != nil {
		log.Warn("list namespace failed, err: %v", err)
		return nil, err
	}

	// query remote namespace models in worker goroutines
	nameC := make(chan string)
	namespaceC := make(chan *models.Namespace)
	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			client := models.NewClient(cfg.ConfigType, cfg.CoordinatorAddr, cfg.UserName, cfg.Password, root)
			store := models.NewStore(client)
			defer store.Close()
			defer wg.Done()
			for name := range nameC {
				namespace, e := store.LoadNamespace(cfg.EncryptKey, name)
				if e != nil {
					log.Warn("load namespace %s failed, err: %v", name, err)
					// assign extent err out of this scope
					err = e
					return
				}
				// verify namespace config
				e = namespace.Verify()
				if e != nil {
					log.Warn("verify namespace %s failed, err: %v", name, e)
					err = e
					return
				}
				namespaceC <- namespace
			}
		}()
	}

	// dispatch goroutine
	go func() {
		for _, name := range names {
			nameC <- name
		}
		close(nameC)
		wg.Wait()
		close(namespaceC)
	}()

	// collect all namespaces
	namespaceModels := make(map[string]*models.Namespace, 64)
	for namespace := range namespaceC {
		namespaceModels[namespace.Name] = namespace
	}
	if err != nil {
		log.Warn("get namespace failed, err:%v", err)
		return nil, err
	}

	return namespaceModels, nil
}

// CreateManager create manager
func CreateManager(cfg *models.Proxy, namespaceConfigs map[string]*models.Namespace) (*Manager, error) {
	m := NewManager()

	// init statistics
	statisticManager, err := CreateStatisticManager(cfg, m)
	if err != nil {
		log.Warn("init stats manager failed, %v", err)
		return nil, err
	}
	m.statistics = statisticManager

	current, _, _ := m.switchIndex.Get()

	// init namespace
	m.namespaces[current] = CreateNamespaceManager(namespaceConfigs)

	// init user
	user, err := CreateUserManager(namespaceConfigs)
	if err != nil {
		return nil, err
	}
	m.users[current] = user

	m.startConnectPoolMetricsTask(cfg.StatsInterval)
	return m, nil
}

// CreateStatisticManager create StatisticManager
func CreateStatisticManager(cfg *models.Proxy, manager *Manager) (*StatisticManager, error) {
	mgr := NewStatisticManager()
	mgr.manager = manager
	mgr.clusterName = cfg.Cluster

	var err error
	if err = mgr.Init(cfg); err != nil {
		return nil, err
	}
	if mgr.generalLogger, err = initGeneralLogger(cfg); err != nil {
		return nil, err
	}
	return mgr, nil
}

func initGeneralLogger(cfg *models.Proxy) (log.Logger, error) {
	c := make(map[string]string, 5)
	c["path"] = cfg.LogPath
	c["filename"] = cfg.LogFileName + "_sql"
	c["level"] = cfg.LogLevel
	c["service"] = cfg.Service
	c["runtime"] = "false"
	return xlog.CreateLogManager(cfg.LogOutput, c)
}

func parseProxyStatsConfig(cfg *models.Proxy) (*ProxyStatsConfig, error) {
	enabled, err := strconv.ParseBool(cfg.StatsEnabled)
	if err != nil {
		return nil, err
	}

	statsConfig := &ProxyStatsConfig{
		Service:      cfg.Service,
		StatsEnabled: enabled,
	}
	return statsConfig, nil
}

// Init init StatisticManager
func (s *StatisticManager) Init(cfg *models.Proxy) error {
	s.closeChan = make(chan bool, 0)
	s.handlers = make(map[string]http.Handler)
	s.slowSQLTime = cfg.SlowSQLTime
	statsCfg, err := parseProxyStatsConfig(cfg)
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

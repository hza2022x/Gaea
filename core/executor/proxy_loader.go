package executor

import (
	"github.com/XiaoMi/Gaea/log"
	"github.com/XiaoMi/Gaea/models"
)

// LoadAndCreateManager load namespace config, and create manager
func LoadAndCreateManager(cfg *models.Proxy) (*Manager, error) {
	namespaceConfigs, err := LoadAllNamespace(cfg)
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

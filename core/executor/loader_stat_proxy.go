package executor

import (
	"github.com/XiaoMi/Gaea/models"
	"github.com/XiaoMi/Gaea/util/log"
	"github.com/XiaoMi/Gaea/util/log/xlog"
	"strconv"
)

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

func ParseProxyStatsConfig(cfg *models.Proxy) (*ProxyStatsConfig, error) {
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

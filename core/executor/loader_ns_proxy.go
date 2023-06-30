package executor

import (
	"github.com/XiaoMi/Gaea/log"
	"github.com/XiaoMi/Gaea/models"
	"sync"
)

// LoadAllNamespace 通过Proxy加载namespace配置
func LoadAllNamespace(cfg *models.Proxy) (map[string]*models.Namespace, error) {
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

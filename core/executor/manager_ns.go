package executor

import (
	"crypto/md5"
	"fmt"
	"github.com/XiaoMi/Gaea/models"
	"github.com/XiaoMi/Gaea/util/log"
	"sort"
)

// NamespaceManager is the manager that holds all namespaces
type NamespaceManager struct {
	namespaces map[string]*Namespace
}

// NewNamespaceManager constructor of NamespaceManager
func NewNamespaceManager() *NamespaceManager {
	return &NamespaceManager{
		namespaces: make(map[string]*Namespace, 64),
	}
}

// CreateNamespaceManager create NamespaceManager
func CreateNamespaceManager(namespaceConfigs map[string]*models.Namespace) *NamespaceManager {
	nsMgr := NewNamespaceManager()
	for _, config := range namespaceConfigs {
		namespace, err := NewNamespace(config)
		if err != nil {
			log.Warn("create namespace %s failed, err: %v", config.Name, err)
			continue
		}
		nsMgr.namespaces[namespace.name] = namespace
	}
	return nsMgr
}

// ShallowCopyNamespaceManager copy NamespaceManager
func ShallowCopyNamespaceManager(nsMgr *NamespaceManager) *NamespaceManager {
	newNsMgr := NewNamespaceManager()
	for k, v := range nsMgr.namespaces {
		newNsMgr.namespaces[k] = v
	}
	return newNsMgr
}

// RebuildNamespace rebuild namespace
func (n *NamespaceManager) RebuildNamespace(config *models.Namespace) error {
	namespace, err := NewNamespace(config)
	if err != nil {
		log.Warn("create namespace %s failed, err: %v", config.Name, err)
		return err
	}
	n.namespaces[config.Name] = namespace
	return nil
}

// DeleteNamespace delete namespace
func (n *NamespaceManager) DeleteNamespace(ns string) {
	delete(n.namespaces, ns)
}

// GetNamespace get namespace in NamespaceManager
func (n *NamespaceManager) GetNamespace(namespace string) *Namespace {
	return n.namespaces[namespace]
}

// GetNamespaces return all namespaces in NamespaceManager
func (n *NamespaceManager) GetNamespaces() map[string]*Namespace {
	return n.namespaces
}

// ConfigFingerprint return config fingerprint
func (n *NamespaceManager) ConfigFingerprint() string {
	sortedKeys := make([]string, 0)
	for k := range n.GetNamespaces() {
		sortedKeys = append(sortedKeys, k)
	}

	sort.Strings(sortedKeys)

	h := md5.New()
	for _, k := range sortedKeys {
		h.Write(n.GetNamespace(k).DumpToJSON())
	}
	return fmt.Sprintf("%x", h.Sum(nil))
}

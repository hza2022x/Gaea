package executor

import (
	"bytes"
	"github.com/XiaoMi/Gaea/models"
	"github.com/XiaoMi/Gaea/mysql"
)

// UserManager means user for auth
// username+password是全局唯一的, 而username可以对应多个namespace
type UserManager struct {
	users          map[string][]string // key: user name, value: user password, same user may have different password, so array of passwords is needed
	userNamespaces map[string]string   // key: UserName+Password, value: name of namespace
}

// NewUserManager constructor of UserManager
func NewUserManager() *UserManager {
	return &UserManager{
		users:          make(map[string][]string, 64),
		userNamespaces: make(map[string]string, 64),
	}
}

// CreateUserManager create UserManager
func CreateUserManager(namespaceConfigs map[string]*models.Namespace) (*UserManager, error) {
	user := NewUserManager()
	for _, ns := range namespaceConfigs {
		user.addNamespaceUsers(ns)
	}
	return user, nil
}

// CloneUserManager close UserManager
func CloneUserManager(user *UserManager) *UserManager {
	ret := NewUserManager()
	// copy
	for k, v := range user.userNamespaces {
		ret.userNamespaces[k] = v
	}
	for k, v := range user.users {
		users := make([]string, len(v))
		copy(users, v)
		ret.users[k] = users
	}

	return ret
}

// RebuildNamespaceUsers rebuild users in namespace
func (u *UserManager) RebuildNamespaceUsers(namespace *models.Namespace) {
	u.ClearNamespaceUsers(namespace.Name)
	u.addNamespaceUsers(namespace)
}

// ClearNamespaceUsers clear users in namespace
func (u *UserManager) ClearNamespaceUsers(namespace string) {
	for key, ns := range u.userNamespaces {
		if ns == namespace {
			delete(u.userNamespaces, key)

			// delete user password in users
			username, password := getUserAndPasswordFromKey(key)
			var s []string
			for i := range u.users[username] {
				if u.users[username][i] == password {
					s = append(u.users[username][:i], u.users[username][i+1:]...)
				}
			}
			u.users[username] = s
		}
	}
}

func (u *UserManager) addNamespaceUsers(namespace *models.Namespace) {
	for _, user := range namespace.Users {
		key := getUserKey(user.UserName, user.Password)
		u.userNamespaces[key] = namespace.Name
		u.users[user.UserName] = append(u.users[user.UserName], user.Password)
	}
}

// CheckUser check if user in users
func (u *UserManager) CheckUser(user string) bool {
	if _, ok := u.users[user]; ok {
		return true
	}
	return false
}

// CheckPassword check if right password with specific user
func (u *UserManager) CheckPassword(user string, salt, auth []byte) (bool, string) {
	for _, password := range u.users[user] {
		checkAuth := mysql.CalcPassword(salt, []byte(password))
		if bytes.Equal(auth, checkAuth) {
			return true, password
		}
	}
	return false, ""
}

// CheckSha2Password check if right password with specific user
func (u *UserManager) CheckSha2Password(user string, salt, auth []byte) (bool, string) {
	for _, password := range u.users[user] {
		checkAuth := mysql.CalcCachingSha2Password(salt, password)
		if bytes.Equal(auth, checkAuth) {
			return true, password
		}
	}
	return false, ""
}

// GetNamespaceByUser return namespace by user
func (u *UserManager) GetNamespaceByUser(userName, password string) string {
	key := getUserKey(userName, password)
	if name, ok := u.userNamespaces[key]; ok {
		return name
	}
	return ""
}

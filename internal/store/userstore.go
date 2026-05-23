// Package store 提供内存版用户注册表（演示 REST + JWT；生产应换数据库）。
package store

import (
	"strconv"
	"sync"

	"gin-demo/internal/config"
)

// User 对外返回的用户模型（不含密码）。
type User struct {
	ID       string `json:"id"`
	Username string `json:"username"`
}

// UserStore 使用 RWMutex 保护 map：读多写少时用读写锁降低竞争。
type UserStore struct {
	mu      sync.RWMutex
	byName  map[string]User
	secrets map[string]string // username -> password（仅演示）
	nextID  int
}

// NewUserStore 由配置种子初始化账号。
func NewUserStore(seeds []config.UserSeed) *UserStore {
	s := &UserStore{
		byName:  make(map[string]User),
		secrets: make(map[string]string),
	}
	for _, seed := range seeds {
		s.nextID++
		id := strconv.Itoa(s.nextID)
		u := User{ID: id, Username: seed.Username}
		s.byName[seed.Username] = u
		s.secrets[seed.Username] = seed.Password
	}
	return s
}

// Authenticate 校验用户名密码，成功返回用户副本。
func (s *UserStore) Authenticate(username, password string) (User, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	u, ok := s.byName[username]
	if !ok {
		return User{}, false
	}
	if s.secrets[username] != password {
		return User{}, false
	}
	return u, true
}

// List 返回全部用户（演示 REST GET）。
func (s *UserStore) List() []User {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]User, 0, len(s.byName))
	for _, u := range s.byName {
		out = append(out, u)
	}
	return out
}

package balance

import (
	"context"
	"strings"
	"sync"
)

// Registry 内存缓存已注册的托管地址，供 Indexer/Tracker 高频过滤。
type Registry struct {
	mu   sync.RWMutex
	set  map[string]struct{}
	store *Store
}

func NewRegistry(store *Store) *Registry {
	return &Registry{store: store, set: make(map[string]struct{})}
}

// Reload 从 DB 加载全部 enabled 托管地址。
func (r *Registry) Reload(ctx context.Context) error {
	addrs, err := r.store.ListEnabledAddresses(ctx, 0)
	if err != nil {
		return err
	}
	next := make(map[string]struct{}, len(addrs))
	for _, a := range addrs {
		next[strings.ToLower(a)] = struct{}{}
	}
	r.mu.Lock()
	r.set = next
	r.mu.Unlock()
	return nil
}

func (r *Registry) Add(address string) {
	r.mu.Lock()
	r.set[strings.ToLower(address)] = struct{}{}
	r.mu.Unlock()
}

func (r *Registry) Remove(address string) {
	r.mu.Lock()
	delete(r.set, strings.ToLower(address))
	r.mu.Unlock()
}

func (r *Registry) IsRegistered(address string) bool {
	if address == "" {
		return false
	}
	r.mu.RLock()
	_, ok := r.set[strings.ToLower(address)]
	r.mu.RUnlock()
	return ok
}

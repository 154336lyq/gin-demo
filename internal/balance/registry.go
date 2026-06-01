package balance

import (
	"context"
	"strings"
	"sync"
)

// Registry 内存缓存已注册的托管地址，供 Indexer/Tracker 高频过滤。
type Registry struct {
	mu      sync.RWMutex
	set     map[string]struct{}
	wallets map[string]CustodialWallet
	store   *Store
}

func NewRegistry(store *Store) *Registry {
	return &Registry{
		store:   store,
		set:     make(map[string]struct{}),
		wallets: make(map[string]CustodialWallet),
	}
}

// Reload 从 DB 加载全部 enabled 托管地址。
func (r *Registry) Reload(ctx context.Context) error {
	rows, err := r.store.ListWallets(ctx, "", "", true, 0)
	if err != nil {
		return err
	}
	next := make(map[string]struct{}, len(rows))
	wmap := make(map[string]CustodialWallet, len(rows))
	for _, w := range rows {
		key := strings.ToLower(w.Address)
		next[key] = struct{}{}
		wmap[key] = w
	}
	r.mu.Lock()
	r.set = next
	r.wallets = wmap
	r.mu.Unlock()
	return nil
}

// Upsert 原子更新内存缓存（注册/变更后立即调用，避免 set 与 wallets 不一致）。
func (r *Registry) Upsert(w CustodialWallet) {
	key := strings.ToLower(w.Address)
	r.mu.Lock()
	defer r.mu.Unlock()
	if !w.Enabled {
		delete(r.set, key)
		delete(r.wallets, key)
		return
	}
	r.set[key] = struct{}{}
	r.wallets[key] = w
}

func (r *Registry) Add(address string) {
	if r.store == nil {
		return
	}
	w, err := r.store.GetWallet(context.Background(), address)
	if err != nil {
		return
	}
	r.Upsert(w)
}

func (r *Registry) Remove(address string) {
	key := strings.ToLower(address)
	r.mu.Lock()
	delete(r.set, key)
	delete(r.wallets, key)
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

// Get 返回托管地址详情（含 user_id、wallet_type）。
func (r *Registry) Get(address string) (CustodialWallet, bool) {
	if address == "" {
		return CustodialWallet{}, false
	}
	r.mu.RLock()
	w, ok := r.wallets[strings.ToLower(address)]
	r.mu.RUnlock()
	return w, ok
}

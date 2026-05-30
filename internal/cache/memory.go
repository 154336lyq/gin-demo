package cache

import (
	"context"
	"sync"
	"time"
)

type memEntry struct {
	val       string
	expiresAt time.Time
}

type memoryCache struct {
	mu   sync.RWMutex
	data map[string]memEntry
}

func newMemoryCache() *memoryCache {
	return &memoryCache{data: make(map[string]memEntry)}
}

func (m *memoryCache) BackendName() string { return "memory" }

func (m *memoryCache) Ping(_ context.Context) error { return nil }

func (m *memoryCache) purgeLocked(now time.Time) {
	for k, v := range m.data {
		if !v.expiresAt.IsZero() && now.After(v.expiresAt) {
			delete(m.data, k)
		}
	}
}

func (m *memoryCache) Get(_ context.Context, key string) (string, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	e, ok := m.data[key]
	if !ok {
		return "", nil
	}
	if !e.expiresAt.IsZero() && time.Now().After(e.expiresAt) {
		return "", nil
	}
	return e.val, nil
}

func (m *memoryCache) Set(_ context.Context, key, val string, ttl time.Duration) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	e := memEntry{val: val}
	if ttl > 0 {
		e.expiresAt = time.Now().Add(ttl)
	}
	m.data[key] = e
	return nil
}

func (m *memoryCache) SetNX(_ context.Context, key, val string, ttl time.Duration) (bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	now := time.Now()
	m.purgeLocked(now)
	if e, ok := m.data[key]; ok {
		if e.expiresAt.IsZero() || now.Before(e.expiresAt) {
			return false, nil
		}
	}
	e := memEntry{val: val}
	if ttl > 0 {
		e.expiresAt = now.Add(ttl)
	}
	m.data[key] = e
	return true, nil
}

func (m *memoryCache) Del(_ context.Context, keys ...string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, k := range keys {
		delete(m.data, k)
	}
	return nil
}

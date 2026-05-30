// Package cache 提供 Redis 缓存与进程内降级实现，供 indexer 去重、锁与读加速。
package cache

import (
	"context"
	"log"
	"time"

	"gin-demo/internal/config"
)

// Cache 抽象 Redis 常用能力；indexer 只依赖此接口，便于本地无 Redis 时降级。
type Cache interface {
	Ping(ctx context.Context) error
	Get(ctx context.Context, key string) (string, error)
	Set(ctx context.Context, key, val string, ttl time.Duration) error
	SetNX(ctx context.Context, key, val string, ttl time.Duration) (bool, error)
	Del(ctx context.Context, keys ...string) error
	BackendName() string
}

// New 优先连接 Redis；失败或未启用时使用内存实现（代码演示，进程重启后失效）。
func New(cfg *config.Config) Cache {
	if cfg.Redis.Enabled {
		rc := newRedisCache(cfg)
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		if pingErr := rc.Ping(ctx); pingErr == nil {
			log.Printf("[cache] 已连接 Redis %s", cfg.Redis.Addr)
			return rc
		} else {
			log.Printf("[cache] Redis 不可用 (%v)，降级为内存缓存（演示模式）", pingErr)
		}
	}
	log.Println("[cache] 使用内存缓存（Redis 未启用或连接失败）")
	return newMemoryCache()
}

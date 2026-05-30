package cache

import (
	"context"
	"time"

	"github.com/redis/go-redis/v9"

	"gin-demo/internal/config"
)

type redisCache struct {
	client *redis.Client
}

func newRedisCache(cfg *config.Config) *redisCache {
	return &redisCache{
		client: redis.NewClient(&redis.Options{
			Addr:     cfg.Redis.Addr,
			Password: cfg.Redis.Password,
			DB:       cfg.Redis.DB,
		}),
	}
}

func (r *redisCache) BackendName() string { return "redis" }

func (r *redisCache) Ping(ctx context.Context) error {
	return r.client.Ping(ctx).Err()
}

func (r *redisCache) Get(ctx context.Context, key string) (string, error) {
	val, err := r.client.Get(ctx, key).Result()
	if err == redis.Nil {
		return "", nil
	}
	return val, err
}

func (r *redisCache) Set(ctx context.Context, key, val string, ttl time.Duration) error {
	return r.client.Set(ctx, key, val, ttl).Err()
}

func (r *redisCache) SetNX(ctx context.Context, key, val string, ttl time.Duration) (bool, error) {
	return r.client.SetNX(ctx, key, val, ttl).Result()
}

func (r *redisCache) Del(ctx context.Context, keys ...string) error {
	if len(keys) == 0 {
		return nil
	}
	return r.client.Del(ctx, keys...).Err()
}

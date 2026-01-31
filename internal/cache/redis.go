package cache

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

type RedisCache struct {
	client  *redis.Client
	timeout time.Duration
}

func NewRedisCache(addr, password string, db int, timeout time.Duration) *RedisCache {
	client := redis.NewClient(&redis.Options{
		Addr:         addr,
		Password:     password,
		DB:           db,
		DialTimeout:  5 * time.Second,
		ReadTimeout:  3 * time.Second,
		WriteTimeout: 3 * time.Second,
		PoolSize:     10,
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := client.Ping(ctx).Err(); err != nil {
		fmt.Printf("Redis connection failed: %v\n", err)
		return &RedisCache{client: nil, timeout: timeout}
	}

	return &RedisCache{client: client, timeout: timeout}
}

func (c *RedisCache) Get(ctx context.Context, key string, dest interface{}) (bool, error) {
	if c.client == nil {
		return false, nil
	}

	val, err := c.client.Get(ctx, key).Result()
	if err == redis.Nil {
		return false, nil
	}
	if err != nil {
		return false, err
	}

	if err := json.Unmarshal([]byte(val), dest); err != nil {
		return false, err
	}

	return true, nil
}

// Fixed signature - single TTL parameter, not variadic
func (c *RedisCache) Set(ctx context.Context, key string, value interface{}, ttl ...time.Duration) error {
	if c.client == nil {
		return nil
	}

	data, err := json.Marshal(value)
	if err != nil {
		return err
	}

	expiration := c.timeout
	if len(ttl) > 0 {
		expiration = ttl[0]
	}

	return c.client.Set(ctx, key, data, expiration).Err()
}

func (c *RedisCache) Delete(ctx context.Context, key string) error {
	if c.client == nil {
		return nil
	}
	return c.client.Del(ctx, key).Err()
}

func (c *RedisCache) IsAvailable() bool {
	if c.client == nil {
		return false
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	return c.client.Ping(ctx).Err() == nil
}

func (c *RedisCache) GenerateCacheKey(tableName string, filters map[string]string, cursor string, limit int, sortBy string, sortDir string) string {
	data, _ := json.Marshal(map[string]interface{}{
		"table":    tableName,
		"filters":  filters,
		"cursor":   cursor,
		"limit":    limit,
		"sort_by":  sortBy,  // ✅ ADD THIS
		"sort_dir": sortDir, // ✅ ADD THIS
	})
	return fmt.Sprintf("cache:%s:%s", tableName, string(data))
}

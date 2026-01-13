package cache

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
)

type RedisCache struct {
	client *redis.Client
	ttl    time.Duration
}

func NewRedisCache(addr, password string, db int, ttl time.Duration) (*RedisCache, error) {
	client := redis.NewClient(&redis.Options{
		Addr:         addr,
		Password:     password,
		DB:           db,
		DialTimeout:  5 * time.Second,
		ReadTimeout:  3 * time.Second,
		WriteTimeout: 3 * time.Second,
		PoolSize:     10,
		MinIdleConns: 5,
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := client.Ping(ctx).Err(); err != nil {
		return &RedisCache{client: nil, ttl: ttl}, nil
	}

	return &RedisCache{client: client, ttl: ttl}, nil
}

func (c *RedisCache) GenerateCacheKey(endpoint string, filters map[string]string, cursor string, limit int) string {
	var filterParts []string
	keys := make([]string, 0, len(filters))
	for k := range filters {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	for _, k := range keys {
		if filters[k] != "" {
			filterParts = append(filterParts, fmt.Sprintf("%s=%s", k, filters[k]))
		}
	}

	keyData := fmt.Sprintf("%s|%s|%s|%d", endpoint, strings.Join(filterParts, "&"), cursor, limit)
	hash := sha256.Sum256([]byte(keyData))
	return fmt.Sprintf("api:%s:%s", endpoint, hex.EncodeToString(hash[:8]))
}

func (c *RedisCache) Get(ctx context.Context, key string, dest interface{}) (bool, error) {
	if c.client == nil {
		return false, nil
	}

	data, err := c.client.Get(ctx, key).Bytes()
	if err != nil {
		if err == redis.Nil {
			return false, nil
		}
		return false, nil
	}

	if err := json.Unmarshal(data, dest); err != nil {
		return false, nil
	}

	return true, nil
}

func (c *RedisCache) Set(ctx context.Context, key string, value interface{}) error {
	if c.client == nil {
		return nil
	}

	data, err := json.Marshal(value)
	if err != nil {
		return fmt.Errorf("failed to marshal value: %w", err)
	}

	return c.client.Set(ctx, key, data, c.ttl).Err()
}

func (c *RedisCache) Delete(ctx context.Context, pattern string) error {
	if c.client == nil {
		return nil
	}

	iter := c.client.Scan(ctx, 0, pattern, 100).Iterator()
	for iter.Next(ctx) {
		c.client.Del(ctx, iter.Val())
	}
	return iter.Err()
}

func (c *RedisCache) Close() error {
	if c.client == nil {
		return nil
	}
	return c.client.Close()
}

func (c *RedisCache) IsAvailable() bool {
	return c.client != nil
}

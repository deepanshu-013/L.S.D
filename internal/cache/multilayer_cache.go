package cache

import (
	"context"
	"encoding/json"
	"sync"
	"time"
)

type MultiLayerCache struct {
	redis      *RedisCache
	localCache *sync.Map
	localTTL   time.Duration
}

type cacheEntry struct {
	data      []byte
	expiresAt time.Time
}

func NewMultiLayerCache(redis *RedisCache, localTTL time.Duration) *MultiLayerCache {
	if localTTL == 0 {
		localTTL = 1 * time.Minute
	}

	mlc := &MultiLayerCache{
		redis:      redis,
		localCache: &sync.Map{},
		localTTL:   localTTL,
	}

	go mlc.cleanupLoop()

	return mlc
}

func (c *MultiLayerCache) Get(ctx context.Context, key string, dest interface{}) (bool, error) {
	if val, ok := c.localCache.Load(key); ok {
		entry := val.(*cacheEntry)
		if time.Now().Before(entry.expiresAt) {
			if err := json.Unmarshal(entry.data, dest); err == nil {
				return true, nil
			}
		}
		c.localCache.Delete(key)
	}

	if c.redis != nil && c.redis.IsAvailable() {
		found, err := c.redis.Get(ctx, key, dest)
		if found {
			data, _ := json.Marshal(dest)
			c.localCache.Store(key, &cacheEntry{
				data:      data,
				expiresAt: time.Now().Add(c.localTTL),
			})
			return true, err
		}
	}

	return false, nil
}

func (c *MultiLayerCache) Set(ctx context.Context, key string, value interface{}, ttl ...time.Duration) error {
	data, err := json.Marshal(value)
	if err != nil {
		return err
	}

	localExpiry := c.localTTL
	if len(ttl) > 0 && ttl[0] < localExpiry {
		localExpiry = ttl[0]
	}

	c.localCache.Store(key, &cacheEntry{
		data:      data,
		expiresAt: time.Now().Add(localExpiry),
	})

	if c.redis != nil && c.redis.IsAvailable() {
		// Call redis Set without spreading ttl slice
		if len(ttl) > 0 {
			return c.redis.Set(ctx, key, value, ttl[0])
		}
		return c.redis.Set(ctx, key, value)
	}

	return nil
}

func (c *MultiLayerCache) Delete(ctx context.Context, key string) error {
	c.localCache.Delete(key)
	if c.redis != nil && c.redis.IsAvailable() {
		return c.redis.Delete(ctx, key)
	}
	return nil
}

func (c *MultiLayerCache) GenerateCacheKey(tableName string, filters map[string]string, cursor string, limit int, sortBy string, sortDir string) string {
	if c.redis != nil {
		return c.redis.GenerateCacheKey(tableName, filters, cursor, limit, sortBy, sortDir) // ✅ ADD sortBy, sortDir
	}

	data, _ := json.Marshal(map[string]interface{}{
		"table":    tableName,
		"filters":  filters,
		"cursor":   cursor,
		"limit":    limit,
		"sort_by":  sortBy,  // ✅ ADD THIS
		"sort_dir": sortDir, // ✅ ADD THIS
	})
	return string(data)
}

func (c *MultiLayerCache) IsAvailable() bool {
	return c.redis != nil && c.redis.IsAvailable()
}

func (c *MultiLayerCache) cleanupLoop() {
	ticker := time.NewTicker(1 * time.Minute)
	defer ticker.Stop()

	for range ticker.C {
		now := time.Now()
		c.localCache.Range(func(key, value interface{}) bool {
			entry := value.(*cacheEntry)
			if now.After(entry.expiresAt) {
				c.localCache.Delete(key)
			}
			return true
		})
	}
}

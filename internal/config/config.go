package config

import (
	"os"
	"strconv"
	"time"
)

type Config struct {
	DatabaseURL     string
	RedisAddr       string
	RedisPassword   string
	RedisDB         int
	ServerPort      string
	CacheTTL        time.Duration
	MaxPageSize     int
	DefaultPageSize int
	QueryTimeout    time.Duration
	RateLimitRPS    int
	SessionSecret   string
}

func Load() *Config {
	return &Config{
		DatabaseURL:     getEnv("DATABASE_URL", ""),
		RedisAddr:       getEnv("REDIS_ADDR", "localhost:6379"),
		RedisPassword:   getEnv("REDIS_PASSWORD", ""),
		RedisDB:         getEnvInt("REDIS_DB", 0),
		ServerPort:      getEnv("PORT", "5000"),
		CacheTTL:        time.Duration(getEnvInt("CACHE_TTL_SECONDS", 30)) * time.Second,
		MaxPageSize:     50,
		DefaultPageSize: 20,
		QueryTimeout:    time.Duration(getEnvInt("QUERY_TIMEOUT_SECONDS", 10)) * time.Second,
		RateLimitRPS:    getEnvInt("RATE_LIMIT_RPS", 100),
		SessionSecret:   getEnv("SESSION_SECRET", ""),
	}
}

func getEnv(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}

func getEnvInt(key string, defaultValue int) int {
	if value := os.Getenv(key); value != "" {
		if intValue, err := strconv.Atoi(value); err == nil {
			return intValue
		}
	}
	return defaultValue
}

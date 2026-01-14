package config

import (
	"os"
	"strconv"
	"strings"
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

	ClickHouseAddr     string
	ClickHouseDatabase string
	ClickHouseUsername string
	ClickHousePassword string
	ClickHouseTables   []string
	CDCSyncInterval    time.Duration
	CDCBatchSize       int
}

func Load() *Config {
	tablesStr := getEnv("CLICKHOUSE_SYNC_TABLES", "")
	var tables []string
	if tablesStr != "" {
		tables = strings.Split(tablesStr, ",")
		for i := range tables {
			tables[i] = strings.TrimSpace(tables[i])
		}
	}

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

		ClickHouseAddr:     getEnv("CLICKHOUSE_ADDR", ""),
		ClickHouseDatabase: getEnv("CLICKHOUSE_DATABASE", "default"),
		ClickHouseUsername: getEnv("CLICKHOUSE_USERNAME", "default"),
		ClickHousePassword: getEnv("CLICKHOUSE_PASSWORD", ""),
		ClickHouseTables:   tables,
		CDCSyncInterval:    time.Duration(getEnvInt("CDC_SYNC_INTERVAL_SECONDS", 30)) * time.Second,
		CDCBatchSize:       getEnvInt("CDC_BATCH_SIZE", 1000),
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

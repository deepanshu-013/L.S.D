package config

import (
	"fmt"
	"log"
	"os"
	"strconv"

	"github.com/joho/godotenv"
)

type Config struct {
	Port               string
	DatabaseURL        string
	ReadReplicaDSN     string
	RedisAddr          string
	RedisPassword      string
	RedisDB            int
	ClickHouseAddr     string
	ClickHouseDB       string
	ClickHouseUser     string
	ClickHousePassword string
	ClickHouseDSN      string // <--- ADD THIS: For Async Inserts Optimization
	EnableCDC          bool
}

func LoadConfig() *Config {
	if err := godotenv.Load(); err != nil {
		log.Println("No .env file found, using environment variables")
	}

	addr := getEnv("CLICKHOUSE_ADDR", "localhost:9000")

	cfg := &Config{
		Port:               getEnv("PORT", "8080"),
		DatabaseURL:        getEnv("DATABASE_URL", "postgres://postgres:password@localhost:5432/postgres?sslmode=disable"),
		ReadReplicaDSN:     getEnv("READ_REPLICA_DSN", ""),
		RedisAddr:          getEnv("REDIS_ADDR", "localhost:6379"),
		RedisPassword:      getEnv("REDIS_PASSWORD", ""),
		RedisDB:            getEnvAsInt("REDIS_DB", 0),
		ClickHouseAddr:     addr,
		ClickHouseDB:       getEnv("CLICKHOUSE_DB", "default"),
		ClickHouseUser:     getEnv("CLICKHOUSE_USER", "default"),
		ClickHousePassword: getEnv("CLICKHOUSE_PASSWORD", ""),
		EnableCDC:          getEnv("ENABLE_CDC", "true") == "true",
	}

	// ═══════════════════════════════════════════════════════════
	// ⭐ OPTIMIZATION: Build DSN with Async Inserts
	// async_insert=1: Batches data in RAM before writing to disk
	// wait_for_async_insert=0: Don't wait for disk, return immediately (Fastest CDC)
	// ═══════════════════════════════════════════════════════════
	cfg.ClickHouseDSN = fmt.Sprintf(
		"tcp://%s?database=%s&username=%s&password=%s&async_insert=1&wait_for_async_insert=1",
		addr,
		cfg.ClickHouseDB,
		cfg.ClickHouseUser,
		cfg.ClickHousePassword,
	)

	return cfg
}

func getEnv(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}

func getEnvAsInt(key string, defaultValue int) int {
	valueStr := getEnv(key, "")
	if value, err := strconv.Atoi(valueStr); err == nil {
		return value
	}
	return defaultValue
}

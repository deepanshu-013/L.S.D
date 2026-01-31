package clickhouse

import (
	"context"
	"fmt"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

type Connection struct {
	conn      driver.Conn
	available bool
}

type Config struct {
	Addr     string
	Database string
	Username string
	Password string
}

func NewConnection(cfg Config) (*Connection, error) {
	if cfg.Addr == "" {
		return &Connection{available: false}, nil
	}

	conn, err := clickhouse.Open(&clickhouse.Options{
		Addr: []string{cfg.Addr},
		Auth: clickhouse.Auth{
			Database: cfg.Database,
			Username: cfg.Username,
			Password: cfg.Password,
		},
		Settings: clickhouse.Settings{
			"max_execution_time":                         60,
			"max_memory_usage":                           10000000000, // 10GB
			"max_threads":                                16,
			"use_uncompressed_cache":                     1,
			"allow_experimental_projection_optimization": 1,
		},
		DialTimeout:     5 * time.Second,
		MaxOpenConns:    20,
		MaxIdleConns:    10,
		ConnMaxLifetime: time.Hour,
		Compression: &clickhouse.Compression{
			Method: clickhouse.CompressionLZ4,
		},
	})
	if err != nil {
		return &Connection{available: false}, nil
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := conn.Ping(ctx); err != nil {
		return &Connection{available: false}, nil
	}

	return &Connection{conn: conn, available: true}, nil
}

func (c *Connection) IsAvailable() bool {
	return c.available
}

func (c *Connection) Query(ctx context.Context, query string, args ...interface{}) (driver.Rows, error) {
	if !c.available {
		return nil, fmt.Errorf("clickhouse not available")
	}
	return c.conn.Query(ctx, query, args...)
}

func (c *Connection) QueryRow(ctx context.Context, query string, args ...interface{}) driver.Row {
	if !c.available {
		return nil
	}
	return c.conn.QueryRow(ctx, query, args...)
}

func (c *Connection) PrepareBatch(ctx context.Context, query string, opts ...driver.PrepareBatchOption) (driver.Batch, error) {
	if !c.available {
		return nil, fmt.Errorf("clickhouse not available")
	}
	return c.conn.PrepareBatch(ctx, query, opts...)
}

func (c *Connection) Exec(ctx context.Context, query string, args ...interface{}) error {
	if !c.available {
		return fmt.Errorf("clickhouse not available")
	}
	return c.conn.Exec(ctx, query, args...)
}

func (c *Connection) Close() error {
	if c.conn != nil {
		return c.conn.Close()
	}
	return nil
}

func (c *Connection) Conn() driver.Conn {
	return c.conn
}

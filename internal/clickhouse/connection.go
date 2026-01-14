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
			"max_execution_time": 60,
		},
		DialTimeout:     5 * time.Second,
		MaxOpenConns:    10,
		MaxIdleConns:    5,
		ConnMaxLifetime: time.Hour,
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

package clickhouse

import (
	"context"
	"sync"
	"sync/atomic"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

type ConnectionPool struct {
	connections []*Connection
	counter     uint64
	available   bool
	mu          sync.RWMutex
}

func NewConnectionPool(cfg Config, poolSize int) (*ConnectionPool, error) {
	if poolSize <= 0 {
		poolSize = 3
	}

	pool := &ConnectionPool{
		connections: make([]*Connection, 0, poolSize),
		available:   false,
	}

	for i := 0; i < poolSize; i++ {
		conn, err := NewConnection(cfg)
		if err != nil || !conn.IsAvailable() {
			continue
		}
		pool.connections = append(pool.connections, conn)
	}

	if len(pool.connections) > 0 {
		pool.available = true
	}

	return pool, nil
}

func (p *ConnectionPool) GetConnection() *Connection {
	if !p.available || len(p.connections) == 0 {
		return &Connection{available: false}
	}

	idx := atomic.AddUint64(&p.counter, 1) % uint64(len(p.connections))
	return p.connections[idx]
}

func (p *ConnectionPool) IsAvailable() bool {
	return p.available
}

func (p *ConnectionPool) Query(ctx context.Context, query string, args ...interface{}) (driver.Rows, error) {
	return p.GetConnection().Query(ctx, query, args...)
}

func (p *ConnectionPool) QueryRow(ctx context.Context, query string, args ...interface{}) driver.Row {
	return p.GetConnection().QueryRow(ctx, query, args...)
}

func (p *ConnectionPool) Exec(ctx context.Context, query string, args ...interface{}) error {
	return p.GetConnection().Exec(ctx, query, args...)
}

func (p *ConnectionPool) PrepareBatch(ctx context.Context, query string, opts ...driver.PrepareBatchOption) (driver.Batch, error) {
	return p.GetConnection().PrepareBatch(ctx, query, opts...)
}

func (p *ConnectionPool) Conn() driver.Conn {
	return p.GetConnection().Conn()
}

func (p *ConnectionPool) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()

	var lastErr error
	for _, conn := range p.connections {
		if err := conn.Close(); err != nil {
			lastErr = err
		}
	}
	p.available = false
	return lastErr
}

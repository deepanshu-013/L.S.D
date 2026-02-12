package pipeline

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	"highperf-api/internal/clickhouse"
	"highperf-api/internal/schema"

	"github.com/jackc/pgx/v5/pgxpool"
)

// CDCEvent represents a change from Postgres
type CDCEvent struct {
	TableName string                 `json:"table_name"`
	Op        string                 `json:"op"` // "INSERT", "UPDATE", "DELETE"
	Data      map[string]interface{} `json:"data"`
	Timestamp time.Time              `json:"timestamp"`
}

// Pipeline manages the flow of data
type Pipeline struct {
	pgPool   *pgxpool.Pool
	chRepo   *clickhouse.SearchRepository
	registry *schema.SchemaRegistry

	// Channels
	eventChan chan CDCEvent   // Raw changes from PG
	batchChan chan []CDCEvent // Normalized batches
	stopChan  chan struct{}
	wg        sync.WaitGroup

	// Config
	workers   int
	batchSize int
	flushTime time.Duration
}

func NewPipeline(pgPool *pgxpool.Pool, chRepo *clickhouse.SearchRepository, registry *schema.SchemaRegistry) *Pipeline {
	return &Pipeline{
		pgPool:   pgPool,
		chRepo:   chRepo,
		registry: registry,
		// Buffered channels prevent blocking
		eventChan: make(chan CDCEvent, 10000),
		batchChan: make(chan []CDCEvent, 100),
		workers:   3,   // Number of consumers writing to ClickHouse
		batchSize: 500, // Use small batches because Async Inserts handles the buffering
		flushTime: 1 * time.Second,
	}
}

// Start fires up the pipeline stages
func (p *Pipeline) Start() {
	p.wg.Add(1)
	go p.poller() // Stage 1: Read PG

	// Start Consumers (Stage 3: Write CH)
	for i := 0; i < p.workers; i++ {
		p.wg.Add(1)
		go p.worker()
	}

	// Start Batcher (Stage 2: Group events)
	p.wg.Add(1)
	go p.batcher()
}

func (p *Pipeline) Stop() {
	close(p.stopChan)
	p.wg.Wait()
}

// STAGE 1: The Poller (Fast, Optimized Polling)
func (p *Pipeline) poller() {
	defer p.wg.Done()
	ticker := time.NewTicker(10 * time.Second) // Poll every 10s

	for {
		select {
		case <-p.stopChan:
			return
		case <-ticker.C:
			p.fetchChanges()
		}
	}
}

func (p *Pipeline) fetchChanges() {
	tables := p.registry.GetAllTables()
	ctx := context.Background()

	for _, table := range tables {
		// Optimized query: Only fetch what we need
		// We assume standard names now (s_indx, updated_at)
		query := fmt.Sprintf(`
            SELECT s_indx, updated_at, is_deleted 
            FROM %s 
            WHERE updated_at > $1 - INTERVAL '10 seconds' 
            ORDER BY updated_at ASC 
            LIMIT 1000
        `, table.Name)

		rows, err := p.pgPool.Query(ctx, query, time.Now())
		if err != nil {
			log.Printf("Poller error for %s: %v", table.Name, err)
			continue
		}

		var events []CDCEvent
		for rows.Next() {
			var id uint64
			var t time.Time
			var del bool

			if err := rows.Scan(&id, &t, &del); err != nil {
				continue
			}

			// Fetch full row if needed (async fetch to keep poller fast)
			// Or just pass the ID to the next stage.
			// For simplicity, we construct the event here:
			events = append(events, CDCEvent{
				TableName: table.Name,
				Op:        "UPSERT", // We treat everything as upsert due to MergeTree
				Data: map[string]interface{}{
					"s_indx":     id,
					"updated_at": t,
					"is_deleted": del,
				},
				Timestamp: t,
			})
		}

		// Push to batcher
		for _, e := range events {
			p.eventChan <- e
		}
	}
}

// STAGE 2: The Batcher (Groups events to save API calls)
func (p *Pipeline) batcher() {
	defer p.wg.Done()

	buf := make([]CDCEvent, 0, p.batchSize)
	timer := time.NewTimer(p.flushTime)

	flush := func() {
		if len(buf) > 0 {
			p.batchChan <- buf
			buf = make([]CDCEvent, 0, p.batchSize)
		}
		timer.Reset(p.flushTime)
	}

	for {
		select {
		case <-p.stopChan:
			flush()
			return
		case event := <-p.eventChan:
			buf = append(buf, event)
			if len(buf) >= p.batchSize {
				flush()
			}
		case <-timer.C:
			flush()
		}
	}
}

// STAGE 3: The Worker (Writes to ClickHouse)
// This is where the complexity lives, but it's isolated here.
func (p *Pipeline) worker() {
	defer p.wg.Done()

	for batch := range p.batchChan {
		// 1. Group by table
		byTable := make(map[string][]map[string]interface{})
		for _, e := range batch {
			byTable[e.TableName] = append(byTable[e.TableName], e.Data)
		}

		// 2. Process each table
		for tableName, records := range byTable {
			// Fetch full data if needed (Optimization: Do this in parallel)
			fullRecords := p.enrichRecords(tableName, records)

			// 3. Index Raw Data (Territory)
			if err := p.chRepo.BulkIndex(context.Background(), tableName, fullRecords); err != nil {
				log.Printf("Worker Error BulkIndex: %v", err)
			}

			// 4. Index Tokens (Map)
			if err := p.chRepo.BulkIndexTokens(context.Background(), tableName, fullRecords); err != nil {
				log.Printf("Worker Error BulkIndexTokens: %v", err)
			}
		}
	}
}

func (p *Pipeline) enrichRecords(tableName string, keys []map[string]interface{}) []map[string]interface{} {
	// Implementation: Fetch full rows from PG based on s_indx
	// This is where you get the full JSON for ClickHouse
	return keys // Placeholder
}

package clickhouse

import (
        "context"
        "encoding/json"
        "fmt"
        "log"
        "sync"
        "time"

        "github.com/jackc/pgx/v5/pgxpool"
        "highperf-api/internal/schema"
)

type CDCManager struct {
        pgPool     *pgxpool.Pool
        chRepo     *SearchRepository
        registry   *schema.SchemaRegistry
        syncTables []string
        batchSize  int
        interval   time.Duration
        stopChan   chan struct{}
        wg         sync.WaitGroup
}

type CDCConfig struct {
        BatchSize    int
        SyncInterval time.Duration
        Tables       []string
}

func NewCDCManager(pgPool *pgxpool.Pool, chRepo *SearchRepository, registry *schema.SchemaRegistry, cfg CDCConfig) *CDCManager {
        if cfg.BatchSize <= 0 {
                cfg.BatchSize = 1000
        }
        if cfg.SyncInterval <= 0 {
                cfg.SyncInterval = 30 * time.Second
        }

        return &CDCManager{
                pgPool:     pgPool,
                chRepo:     chRepo,
                registry:   registry,
                syncTables: cfg.Tables,
                batchSize:  cfg.BatchSize,
                interval:   cfg.SyncInterval,
                stopChan:   make(chan struct{}),
        }
}

func (m *CDCManager) Start() {
        if !m.chRepo.IsAvailable() {
                log.Println("ClickHouse not available, CDC sync disabled")
                return
        }

        m.wg.Add(1)
        go m.syncLoop()
        log.Printf("CDC sync started for %d tables", len(m.syncTables))
}

func (m *CDCManager) Stop() {
        close(m.stopChan)
        m.wg.Wait()
        log.Println("CDC sync stopped")
}

func (m *CDCManager) syncLoop() {
        defer m.wg.Done()

        for _, tableName := range m.syncTables {
                m.initialSync(tableName)
        }

        ticker := time.NewTicker(m.interval)
        defer ticker.Stop()

        for {
                select {
                case <-m.stopChan:
                        return
                case <-ticker.C:
                        for _, tableName := range m.syncTables {
                                m.incrementalSync(tableName)
                        }
                }
        }
}

func (m *CDCManager) initialSync(tableName string) error {
        ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
        defer cancel()

        table := m.registry.GetTable(tableName)
        if table == nil {
                return fmt.Errorf("table not found: %s", tableName)
        }

        if err := m.chRepo.EnsureSearchIndex(ctx, tableName); err != nil {
                log.Printf("Failed to create search index for %s: %v", tableName, err)
                return err
        }

        var columns []string
        for _, col := range table.Columns {
                columns = append(columns, col.Name)
        }

        pkColumn := "id"
        if len(table.PrimaryKey) > 0 {
                pkColumn = table.PrimaryKey[0]
        }

        offset := 0
        totalSynced := 0
        startTime := time.Now()

        for {
                query := fmt.Sprintf(`
                        SELECT %s
                        FROM %s
                        ORDER BY %s
                        LIMIT %d OFFSET %d
                `, columnsToSQL(columns), tableName, pkColumn, m.batchSize, offset)

                rows, err := m.pgPool.Query(ctx, query)
                if err != nil {
                        log.Printf("Failed to query %s: %v", tableName, err)
                        return err
                }

                var records []map[string]interface{}
                for rows.Next() {
                        values, err := rows.Values()
                        if err != nil {
                                rows.Close()
                                return err
                        }

                        record := make(map[string]interface{})
                        for i, col := range columns {
                                record[col] = values[i]
                        }
                        records = append(records, record)
                }
                rows.Close()

                if len(records) == 0 {
                        break
                }

                if err := m.chRepo.SyncFromPostgres(ctx, tableName, records); err != nil {
                        log.Printf("Failed to sync batch to ClickHouse: %v", err)
                        return err
                }

                totalSynced += len(records)
                offset += m.batchSize

                if len(records) < m.batchSize {
                        break
                }
        }

        duration := time.Since(startTime)
        log.Printf("Initial sync completed for %s: %d records in %v", tableName, totalSynced, duration)

        return nil
}

func (m *CDCManager) incrementalSync(tableName string) error {
        ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
        defer cancel()

        table := m.registry.GetTable(tableName)
        if table == nil {
                return fmt.Errorf("table not found: %s", tableName)
        }

        var updatedAtColumn string
        for _, col := range table.Columns {
                if col.Name == "updated_at" || col.Name == "modified_at" || col.Name == "last_modified" {
                        updatedAtColumn = col.Name
                        break
                }
        }

        if updatedAtColumn == "" {
                return nil
        }

        var columns []string
        for _, col := range table.Columns {
                columns = append(columns, col.Name)
        }

        cutoffTime := time.Now().Add(-m.interval * 2)

        query := fmt.Sprintf(`
                SELECT %s
                FROM %s
                WHERE %s > $1
                ORDER BY %s
                LIMIT %d
        `, columnsToSQL(columns), tableName, updatedAtColumn, updatedAtColumn, m.batchSize)

        rows, err := m.pgPool.Query(ctx, query, cutoffTime)
        if err != nil {
                return err
        }
        defer rows.Close()

        var records []map[string]interface{}
        for rows.Next() {
                values, err := rows.Values()
                if err != nil {
                        return err
                }

                record := make(map[string]interface{})
                for i, col := range columns {
                        record[col] = values[i]
                }
                records = append(records, record)
        }

        if len(records) > 0 {
                if err := m.chRepo.SyncFromPostgres(ctx, tableName, records); err != nil {
                        return err
                }
                log.Printf("Incremental sync for %s: %d records", tableName, len(records))
        }

        return nil
}

func (m *CDCManager) TriggerSync(tableName string) error {
        return m.initialSync(tableName)
}

func columnsToSQL(columns []string) string {
        return fmt.Sprintf("%s", joinColumns(columns))
}

func joinColumns(columns []string) string {
        result := ""
        for i, col := range columns {
                if i > 0 {
                        result += ", "
                }
                result += col
        }
        return result
}

type CDCEvent struct {
        Table     string                 `json:"table"`
        Operation string                 `json:"operation"`
        Data      map[string]interface{} `json:"data"`
        Timestamp time.Time              `json:"timestamp"`
}

func (m *CDCManager) ProcessEvent(ctx context.Context, event CDCEvent) error {
        if !m.chRepo.IsAvailable() {
                return nil
        }

        table := m.registry.GetTable(event.Table)
        if table == nil {
                return fmt.Errorf("table not found: %s", event.Table)
        }

        pkColumn := "id"
        if len(table.PrimaryKey) > 0 {
                pkColumn = table.PrimaryKey[0]
        }

        id := fmt.Sprintf("%v", event.Data[pkColumn])

        switch event.Operation {
        case "INSERT", "UPDATE":
                return m.chRepo.IndexRecord(ctx, event.Table, id, event.Data)
        case "DELETE":
                return m.chRepo.DeleteRecord(ctx, event.Table, id)
        }

        return nil
}

func ParseCDCPayload(payload []byte) (*CDCEvent, error) {
        var event CDCEvent
        if err := json.Unmarshal(payload, &event); err != nil {
                return nil, err
        }
        event.Timestamp = time.Now()
        return &event, nil
}

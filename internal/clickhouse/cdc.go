package clickhouse

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"strings"
	"sync"
	"time"

	"highperf-api/internal/schema"

	"github.com/jackc/pgx/v5/pgxpool"
)

type CDCConfig struct {
	BatchSize       int
	SyncInterval    time.Duration
	ParallelWorkers int
	ChunkSize       int64
}

type CDCManager struct {
	pgPool       *pgxpool.Pool
	chRepo       *SearchRepository
	entityRepo   *EntityRepository
	registry     *schema.SchemaRegistry
	config       CDCConfig
	stopChan     chan struct{}
	wg           sync.WaitGroup
	mu           sync.RWMutex
	tableStatus  map[string]*TableSyncStatus
	lastSyncedID map[string]int64
	lastSyncTime map[string]time.Time
	globalStatus *CDCStatus
	tables       []string
}

type TableSyncStatus struct {
	TableName       string    `json:"table_name"`
	TotalRows       int64     `json:"total_rows"`
	RecordsIndexed  int64     `json:"records_indexed"`
	LastSyncAt      time.Time `json:"last_sync_at"`
	LastSyncRecords int       `json:"last_sync_records"`
	LastSyncedID    int64     `json:"last_synced_id"`
	IsRunning       bool      `json:"is_running"`
	LastError       string    `json:"last_error,omitempty"`
}

type CDCStatus struct {
	IsRunning     bool                        `json:"is_running"`
	StartedAt     time.Time                   `json:"started_at"`
	TotalTables   int                         `json:"total_tables"`
	SyncInterval  string                      `json:"sync_interval"`
	TableStatuses map[string]*TableSyncStatus `json:"table_statuses"`
}

type CDCEvent struct {
	Table     string                 `json:"table"`
	Operation string                 `json:"operation"`
	Data      map[string]interface{} `json:"data"`
	Timestamp time.Time              `json:"timestamp"`
}

func NewCDCManager(pgPool *pgxpool.Pool, chRepo *SearchRepository, registry *schema.SchemaRegistry, cfg CDCConfig) *CDCManager {
	if cfg.BatchSize <= 0 {
		cfg.BatchSize = 10000
	}
	if cfg.SyncInterval <= 0 {
		cfg.SyncInterval = 30 * time.Second
	}
	if cfg.ParallelWorkers == 0 {
		cfg.ParallelWorkers = 5
	}
	if cfg.ChunkSize == 0 {
		cfg.ChunkSize = 100000
	}

	allTables := registry.GetAllTables()
	var tableNames []string
	for _, table := range allTables {
		tableNames = append(tableNames, table.Name)
	}

	log.Printf("Auto-discovered %d tables for CDC sync: %v", len(tableNames), tableNames)

	tableStatus := make(map[string]*TableSyncStatus)
	for _, t := range tableNames {
		tableStatus[t] = &TableSyncStatus{TableName: t}
	}

	entityRepo := NewEntityRepository(chRepo.conn)

	return &CDCManager{
		pgPool:       pgPool,
		chRepo:       chRepo,
		entityRepo:   entityRepo,
		registry:     registry,
		config:       cfg,
		stopChan:     make(chan struct{}),
		tableStatus:  tableStatus,
		lastSyncedID: make(map[string]int64),
		lastSyncTime: make(map[string]time.Time),
		tables:       tableNames,
		globalStatus: &CDCStatus{
			TotalTables:   len(tableNames),
			SyncInterval:  cfg.SyncInterval.String(),
			TableStatuses: tableStatus,
		},
	}
}

func (m *CDCManager) Start() {
	if !m.chRepo.IsAvailable() {
		log.Println("ClickHouse not available, CDC sync disabled")
		return
	}

	if len(m.tables) == 0 {
		log.Println("No tables found for CDC sync")
		return
	}

	ctx := context.Background()
	if m.entityRepo != nil {
		if err := m.entityRepo.Initialize(ctx); err != nil {
			log.Printf("⚠️  Entity layers not created (continuing without): %v", err)
			m.entityRepo = nil
		}
	}

	m.mu.Lock()
	m.globalStatus.IsRunning = true
	m.globalStatus.StartedAt = time.Now()
	m.mu.Unlock()

	log.Printf("CDC sync started for %d tables with batch size %d", len(m.tables), m.config.BatchSize)
	m.loadCheckpoints()
	m.wg.Add(1)
	go m.syncLoop()
}

func (m *CDCManager) Stop() {
	log.Println("Stopping CDC sync...")
	m.mu.Lock()
	m.globalStatus.IsRunning = false
	m.mu.Unlock()
	close(m.stopChan)
	m.wg.Wait()
	log.Println("CDC sync stopped")
}

func (m *CDCManager) GetStatus() *CDCStatus {
	m.mu.RLock()
	defer m.mu.RUnlock()

	tableStatuses := make(map[string]*TableSyncStatus)
	for name, status := range m.tableStatus {
		copied := &TableSyncStatus{
			TableName:       status.TableName,
			TotalRows:       status.TotalRows,
			RecordsIndexed:  status.RecordsIndexed,
			LastSyncAt:      status.LastSyncAt,
			LastSyncRecords: status.LastSyncRecords,
			LastSyncedID:    status.LastSyncedID,
			IsRunning:       status.IsRunning,
			LastError:       status.LastError,
		}
		tableStatuses[name] = copied
	}

	return &CDCStatus{
		IsRunning:     m.globalStatus.IsRunning,
		StartedAt:     m.globalStatus.StartedAt,
		TotalTables:   m.globalStatus.TotalTables,
		SyncInterval:  m.globalStatus.SyncInterval,
		TableStatuses: tableStatuses,
	}
}

func (m *CDCManager) syncLoop() {
	defer m.wg.Done()

	log.Printf("Starting parallel initial sync with %d workers...", m.config.ParallelWorkers)
	m.parallelInitialSync()
	log.Println("All initial syncs completed. Switching to incremental mode.")

	ticker := time.NewTicker(m.config.SyncInterval)
	defer ticker.Stop()

	for {
		select {
		case <-m.stopChan:
			return
		case <-ticker.C:
			for _, tableName := range m.tables {
				m.incrementalSync(tableName)
			}
		}
	}
}

func (m *CDCManager) parallelInitialSync() {
	var wg sync.WaitGroup
	semaphore := make(chan struct{}, m.config.ParallelWorkers)

	for _, tableName := range m.tables {
		wg.Add(1)
		go func(tName string) {
			defer wg.Done()
			semaphore <- struct{}{}
			defer func() { <-semaphore }()

			if err := m.syncTableChunked(tName); err != nil {
				log.Printf("[%s] Initial sync failed: %v", tName, err)
			}
		}(tableName)
	}

	wg.Wait()
}

func (m *CDCManager) ensureSearchTable(tableName string) error {
	searchTableName := fmt.Sprintf("search_%s", tableName)

	checkQuery := fmt.Sprintf("EXISTS TABLE %s", searchTableName)
	var exists uint8
	row := m.chRepo.conn.QueryRow(context.Background(), checkQuery)
	if err := row.Scan(&exists); err == nil && exists == 1 {
		log.Printf("[%s] ClickHouse search table already exists", tableName)
		return nil
	}

	query := fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s (
			id String,
			table_name String DEFAULT '%s',
			search_text String,
			original_data String CODEC(ZSTD(3)),
			is_deleted UInt8 DEFAULT 0,
			synced_at DateTime DEFAULT now(),
			updated_at DateTime DEFAULT now(),
			
			INDEX idx_search_bloom search_text TYPE tokenbf_v1(32768, 3, 0) GRANULARITY 4,
			INDEX idx_id_minmax id TYPE minmax GRANULARITY 4,
			INDEX idx_date_minmax updated_at TYPE minmax GRANULARITY 1
		) ENGINE = MergeTree()
		PARTITION BY toYYYYMM(updated_at)
		ORDER BY (is_deleted, updated_at, id)
		SETTINGS
			index_granularity = 8192,
			index_granularity_bytes = 10485760,
			compress_marks = 1,
			compress_primary_key = 1
	`, searchTableName, tableName)

	if err := m.chRepo.conn.Exec(context.Background(), query); err != nil {
		return fmt.Errorf("failed to create table: %w", err)
	}

	log.Printf("[%s] ✅ Optimized ClickHouse search table created", tableName)
	return nil
}

func (m *CDCManager) syncTableChunked(tableName string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 72*time.Hour)
	defer cancel()

	m.updateTableStatus(tableName, func(s *TableSyncStatus) {
		s.IsRunning = true
		s.LastError = ""
	})
	defer m.updateTableStatus(tableName, func(s *TableSyncStatus) {
		s.IsRunning = false
	})

	table := m.registry.GetTable(tableName)
	if table == nil {
		err := fmt.Errorf("table not found: %s", tableName)
		m.updateTableStatus(tableName, func(s *TableSyncStatus) { s.LastError = err.Error() })
		return err
	}

	pkCol, err := m.getPrimaryKeyColumn(tableName)
	if err != nil {
		log.Printf("[%s] %v - skipping sync", tableName, err)
		m.updateTableStatus(tableName, func(s *TableSyncStatus) { s.LastError = err.Error() })
		return err
	}

	log.Printf("[%s] Using primary key column: %s", tableName, pkCol)

	if err := m.chRepo.EnsureSearchIndex(ctx, tableName); err != nil {
		log.Printf("[%s] Failed to create search index: %v", tableName, err)
		m.updateTableStatus(tableName, func(s *TableSyncStatus) { s.LastError = err.Error() })
		return err
	}

	var totalRows int64
	countQuery := fmt.Sprintf("SELECT COALESCE(MAX(%s), 0) FROM %s", pkCol, tableName)
	if err := m.pgPool.QueryRow(ctx, countQuery).Scan(&totalRows); err != nil {
		log.Printf("[%s] Failed to get max ID: %v", tableName, err)
	}

	m.updateTableStatus(tableName, func(s *TableSyncStatus) { s.TotalRows = totalRows })

	m.mu.RLock()
	startID := m.lastSyncedID[tableName]
	m.mu.RUnlock()

	if startID >= totalRows {
		log.Printf("[%s] ✅ Already up to date (max %s: %d)", tableName, pkCol, totalRows)
		return nil
	}

	log.Printf("[%s] Syncing from %s %d to %d (total: %d records)",
		tableName, pkCol, startID, totalRows, totalRows-startID)

	var columns []string
	for _, col := range table.Columns {
		columns = append(columns, col.Name)
	}

	columnList := joinColumns(columns)
	totalSynced := int64(0)
	startTime := time.Now()
	lastLogTime := time.Now()

	for currentID := startID; currentID < totalRows; currentID += m.config.ChunkSize {
		endID := currentID + m.config.ChunkSize
		if endID > totalRows {
			endID = totalRows
		}

		query := fmt.Sprintf(`
			SELECT %s
			FROM %s
			WHERE %s > $1 AND %s <= $2
			ORDER BY %s
		`, columnList, tableName, pkCol, pkCol, pkCol)

		rows, err := m.pgPool.Query(ctx, query, currentID, endID)
		if err != nil {
			log.Printf("[%s] Failed to query chunk %d-%d: %v", tableName, currentID, endID, err)
			continue
		}

		var records []map[string]interface{}
		var entities []*Entity

		for rows.Next() {
			values, err := rows.Values()
			if err != nil {
				continue
			}

			record := make(map[string]interface{})
			for i, col := range table.Columns {
				if i < len(values) {
					record[col.Name] = values[i]
				}
			}

			records = append(records, record)

			if m.entityRepo != nil && m.entityRepo.IsEnabled() {
				pkValue := fmt.Sprintf("%v", record[pkCol])
				entity := m.entityRepo.ExtractEntity(tableName, pkValue, record)
				entities = append(entities, entity)
			}
		}

		rows.Close()

		if len(records) > 0 {
			if err := m.chRepo.BulkIndex(ctx, tableName, records); err != nil {
				log.Printf("[%s] Failed to index chunk: %v", tableName, err)
			} else {
				totalSynced += int64(len(records))

				if len(entities) > 0 && m.entityRepo != nil {
					if err := m.entityRepo.BulkIndexEntities(ctx, entities); err != nil {
						log.Printf("[%s] Failed to index entities: %v", tableName, err)
					}
				}

				m.mu.Lock()
				m.lastSyncedID[tableName] = endID
				m.mu.Unlock()

				m.updateTableStatus(tableName, func(s *TableSyncStatus) {
					s.RecordsIndexed += int64(len(records))
					s.LastSyncedID = endID
				})
			}

			if time.Since(lastLogTime) > 10*time.Second {
				elapsed := time.Since(startTime)
				rate := float64(totalSynced) / elapsed.Seconds()
				progress := float64(endID-startID) / float64(totalRows-startID) * 100
				log.Printf("[%s] Progress: %.1f%% (%d/%d records, %.0f rec/sec)",
					tableName, progress, totalSynced, totalRows-startID, rate)
				lastLogTime = time.Now()
			}
		}
	}

	elapsed := time.Since(startTime)
	if totalSynced > 0 {
		log.Printf("[%s] ✅ Initial sync completed: %d records in %v (%.0f rec/sec)",
			tableName, totalSynced, elapsed, float64(totalSynced)/elapsed.Seconds())
	}

	m.updateTableStatus(tableName, func(s *TableSyncStatus) {
		s.LastSyncAt = time.Now()
		s.LastSyncRecords = int(totalSynced)
	})

	return nil
}

func (m *CDCManager) incrementalSync(tableName string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	table := m.registry.GetTable(tableName)
	if table == nil {
		return fmt.Errorf("table not found: %s", tableName)
	}

	pkCol, err := m.getPrimaryKeyColumn(tableName)
	if err != nil {
		return err
	}

	m.updateTableStatus(tableName, func(s *TableSyncStatus) { s.IsRunning = true })
	defer m.updateTableStatus(tableName, func(s *TableSyncStatus) { s.IsRunning = false })

	m.mu.RLock()
	lastID := m.lastSyncedID[tableName]
	m.mu.RUnlock()

	var columns []string
	for _, col := range table.Columns {
		columns = append(columns, col.Name)
	}

	columnList := joinColumns(columns)
	query := fmt.Sprintf(`
		SELECT %s
		FROM %s
		WHERE %s > $1
		ORDER BY %s
		LIMIT $2
	`, columnList, tableName, pkCol, pkCol)

	rows, err := m.pgPool.Query(ctx, query, lastID, m.config.BatchSize)
	if err != nil {
		m.updateTableStatus(tableName, func(s *TableSyncStatus) { s.LastError = err.Error() })
		return err
	}
	defer rows.Close()

	var records []map[string]interface{}
	var newMaxID int64

	for rows.Next() {
		values, err := rows.Values()
		if err != nil {
			continue
		}

		record := make(map[string]interface{})
		for i, col := range table.Columns {
			if i < len(values) {
				record[col.Name] = values[i]
			}
		}

		if idVal, ok := record[pkCol]; ok {
			if id, ok := idVal.(int64); ok && id > newMaxID {
				newMaxID = id
			}
		}

		records = append(records, record)
	}

	if len(records) > 0 {
		if err := m.chRepo.BulkIndex(ctx, tableName, records); err != nil {
			m.updateTableStatus(tableName, func(s *TableSyncStatus) { s.LastError = err.Error() })
			return err
		}

		m.mu.Lock()
		m.lastSyncedID[tableName] = newMaxID
		m.mu.Unlock()

		log.Printf("[%s] Incremental sync: %d new records (last %s: %d)",
			tableName, len(records), pkCol, newMaxID)

		m.updateTableStatus(tableName, func(s *TableSyncStatus) {
			s.RecordsIndexed += int64(len(records))
			s.LastSyncAt = time.Now()
			s.LastSyncRecords = len(records)
			s.LastSyncedID = newMaxID
		})
	}

	return nil
}

func (m *CDCManager) getPrimaryKeyColumn(tableName string) (string, error) {
	table := m.registry.GetTable(tableName)
	if table == nil {
		return "", fmt.Errorf("table not found")
	}

	if len(table.PrimaryKey) == 0 {
		return "", fmt.Errorf("no primary key defined")
	}

	return table.PrimaryKey[0], nil
}

func (m *CDCManager) loadCheckpoints() {
	ctx := context.Background()
	for _, tableName := range m.tables {
		pkCol, err := m.getPrimaryKeyColumn(tableName)
		if err != nil {
			log.Printf("[%s] Skipping checkpoint: %v", tableName, err)
			continue
		}

		searchTable := fmt.Sprintf("search_%s", tableName)
		query := fmt.Sprintf("SELECT max(toInt64(id)) FROM %s WHERE is_deleted = 0", searchTable)

		var maxID int64
		row := m.chRepo.conn.QueryRow(ctx, query)
		if err := row.Scan(&maxID); err == nil && maxID > 0 {
			m.mu.Lock()
			m.lastSyncedID[tableName] = maxID
			m.mu.Unlock()
			log.Printf("[%s] Resuming from %s: %d", tableName, pkCol, maxID)
		}
	}
}

func (m *CDCManager) updateTableStatus(tableName string, update func(*TableSyncStatus)) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if status, ok := m.tableStatus[tableName]; ok {
		update(status)
	}
}

func (m *CDCManager) TriggerSync(tableName string) error {
	return m.syncTableChunked(tableName)
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

func joinColumns(columns []string) string {
	return strings.Join(columns, ", ")
}

func (m *CDCManager) Restart() error {
	log.Println("Restarting CDC sync...")

	if m.globalStatus.IsRunning {
		m.Stop()
		time.Sleep(1 * time.Second)
	}

	allTables := m.registry.GetAllTables()
	var tableNames []string
	for _, table := range allTables {
		tableNames = append(tableNames, table.Name)
	}

	m.mu.Lock()
	m.tables = tableNames
	for _, t := range tableNames {
		if _, exists := m.tableStatus[t]; !exists {
			m.tableStatus[t] = &TableSyncStatus{TableName: t}
			log.Printf("Added new table to CDC: %s", t)
		}
	}
	m.globalStatus.TotalTables = len(tableNames)
	m.globalStatus.TableStatuses = m.tableStatus
	m.mu.Unlock()

	log.Printf("Rediscovered %d tables for CDC sync: %v", len(tableNames), tableNames)
	m.Start()
	return nil
}

func (m *CDCManager) TriggerTableSync(tableName string) error {
	if !m.chRepo.IsAvailable() {
		return fmt.Errorf("ClickHouse not available")
	}

	table := m.registry.GetTable(tableName)
	if table == nil {
		log.Printf("[%s] Not in registry, adding table...", tableName)
		if err := m.registry.AddTable(tableName); err != nil {
			return fmt.Errorf("failed to add table to registry: %w", err)
		}

		table = m.registry.GetTable(tableName)
		if table == nil {
			return fmt.Errorf("table %s not found even after adding", tableName)
		}
	}

	m.mu.Lock()
	if _, exists := m.tableStatus[tableName]; !exists {
		m.tableStatus[tableName] = &TableSyncStatus{TableName: tableName}
		m.tables = append(m.tables, tableName)
		m.globalStatus.TotalTables = len(m.tables)
		log.Printf("Added %s to CDC tracking", tableName)
	}
	m.mu.Unlock()

	log.Printf("Starting immediate sync for table: %s", tableName)
	if err := m.syncTableChunked(tableName); err != nil {
		return fmt.Errorf("sync failed: %w", err)
	}

	log.Printf("✅ Successfully synced %s to ClickHouse", tableName)
	return nil
}

func (m *CDCManager) GetEntityRepository() *EntityRepository {
	return m.entityRepo
}

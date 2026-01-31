package clickhouse

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"log"
	"sort"
	"strings"
	"sync"
	"time"

	"highperf-api/internal/schema"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

type ClickHouseConnection interface {
	Query(ctx context.Context, query string, args ...interface{}) (driver.Rows, error)
	QueryRow(ctx context.Context, query string, args ...interface{}) driver.Row
	Exec(ctx context.Context, query string, args ...interface{}) error
	PrepareBatch(ctx context.Context, query string, opts ...driver.PrepareBatchOption) (driver.Batch, error)
	IsAvailable() bool
	Conn() driver.Conn
}

type SearchRepository struct {
	conn       ClickHouseConnection
	registry   *schema.SchemaRegistry
	entityRepo *EntityRepository
}

func NewSearchRepository(conn ClickHouseConnection, registry *schema.SchemaRegistry) *SearchRepository {
	return &SearchRepository{
		conn:       conn,
		registry:   registry,
		entityRepo: NewEntityRepository(conn),
	}
}

type SearchResult struct {
	Data          []map[string]interface{} `json:"data"`
	NextCursor    string                   `json:"next_cursor,omitempty"`
	HasMore       bool                     `json:"has_more"`
	Count         int                      `json:"count"`
	SearchColumns []string                 `json:"search_columns,omitempty"`
	TablesQueried int                      `json:"tables_queried,omitempty"`
}

type SearchParams struct {
	TableName     string
	SearchTerm    string
	SearchColumns []string
	Cursor        string
	Limit         int
	Filters       map[string]string
	DateFrom      *time.Time
}

type SyncStats struct {
	TableName    string
	RecordCount  int
	SyncDuration time.Duration
	LastSyncAt   time.Time
}

type SearchCursor struct {
	LastID        string    `json:"last_id"`
	LastUpdatedAt time.Time `json:"last_updated_at"`
}

func (r *SearchRepository) IsAvailable() bool {
	return r.conn != nil && r.conn.IsAvailable()
}

func (r *SearchRepository) EnsureSearchIndex(ctx context.Context, tableName string) error {
	if !r.IsAvailable() {
		return fmt.Errorf("clickhouse not available")
	}

	table := r.registry.GetTable(tableName)
	if table == nil {
		return fmt.Errorf("table not found: %s", tableName)
	}

	var textColumns []string
	for _, col := range table.Columns {
		if col.DataType == "character varying" || col.DataType == "text" {
			textColumns = append(textColumns, strings.ToLower(col.Name))
		}
	}

	var columnDefs string
	if len(textColumns) > 0 {
		for _, colName := range textColumns {
			columnDefs += fmt.Sprintf(",\n\t\t\t%s String DEFAULT ''", colName)
		}
	}

	indexTableName := fmt.Sprintf("search_%s", tableName)

	createTableSQL := fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s (
			id String,
			table_name String DEFAULT '%s',
			search_text String,
			original_data String CODEC(ZSTD(3))%s,
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
	`, indexTableName, tableName, columnDefs)

	return r.conn.Exec(ctx, createTableSQL)
}

func (r *SearchRepository) BulkIndex(ctx context.Context, tableName string, records []map[string]interface{}) error {
	if !r.IsAvailable() {
		return nil
	}

	if len(records) == 0 {
		return nil
	}

	if err := r.EnsureSearchIndex(ctx, tableName); err != nil {
		return err
	}

	table := r.registry.GetTable(tableName)
	if table == nil {
		return fmt.Errorf("table not found: %s", tableName)
	}

	pkColumn := "id"
	if len(table.PrimaryKey) > 0 {
		pkColumn = table.PrimaryKey[0]
	}

	var searchableColumns []string
	for _, col := range table.Columns {
		if col.DataType == "character varying" || col.DataType == "text" {
			searchableColumns = append(searchableColumns, strings.ToLower(col.Name))
		}
	}

	indexTableName := fmt.Sprintf("search_%s", tableName)
	insertColumns := "id, table_name, search_text, original_data, is_deleted, updated_at"
	for _, col := range searchableColumns {
		insertColumns += ", " + col
	}

	batch, err := r.conn.PrepareBatch(ctx, fmt.Sprintf(`
		INSERT INTO %s (%s)
	`, indexTableName, insertColumns))
	if err != nil {
		return fmt.Errorf("failed to prepare batch: %w", err)
	}

	startTime := time.Now()

	for _, record := range records {
		pkValue := fmt.Sprintf("%v", record[pkColumn])

		isDeleted := uint8(0)
		if val, ok := record["is_deleted"]; ok {
			if deleted, ok := val.(bool); ok && deleted {
				isDeleted = 1
			}
		}

		searchText := r.extractSearchableText(record, table)
		originalData, _ := json.Marshal(record)

		var updatedAt time.Time
		if val, ok := record["updated_at"]; ok {
			if t, ok := val.(time.Time); ok {
				updatedAt = t
			}
		}
		if updatedAt.IsZero() {
			updatedAt = time.Now()
		}

		values := []interface{}{pkValue, tableName, searchText, string(originalData), isDeleted, updatedAt}

		for _, colName := range searchableColumns {
			colValue := ""
			if val, ok := record[colName]; ok && val != nil {
				colValue = fmt.Sprintf("%v", val)
			} else if val, ok := record[strings.Title(colName)]; ok && val != nil {
				colValue = fmt.Sprintf("%v", val)
			}
			values = append(values, colValue)
		}

		if err := batch.Append(values...); err != nil {
			log.Printf("[BulkIndex] Failed to append record %s: %v", pkValue, err)
		}
	}

	if err := batch.Send(); err != nil {
		log.Printf("[BulkIndex] Batch send failed for %s: %v", tableName, err)
		return fmt.Errorf("batch send failed: %w", err)
	}

	duration := time.Since(startTime)
	recordsPerSecond := float64(len(records)) / duration.Seconds()
	log.Printf("[BulkIndex] %s: %d records in %v (%.0f rec/sec)",
		tableName, len(records), duration, recordsPerSecond)

	return nil
}

func (r *SearchRepository) extractSearchableText(record map[string]interface{}, table *schema.TableInfo) string {
	var textParts []string
	for _, col := range table.Columns {
		if col.DataType == "character varying" || col.DataType == "text" {
			if val, ok := record[col.Name]; ok && val != nil {
				textParts = append(textParts, fmt.Sprintf("%v", val))
			}
		}
	}
	return strings.ToLower(strings.Join(textParts, " "))
}

func (r *SearchRepository) IndexRecord(ctx context.Context, tableName string, id string, record map[string]interface{}) error {
	if !r.IsAvailable() {
		return nil
	}
	return r.BulkIndex(ctx, tableName, []map[string]interface{}{record})
}

func (r *SearchRepository) DeleteRecord(ctx context.Context, tableName string, id string) error {
	if !r.IsAvailable() {
		return nil
	}

	if err := r.EnsureSearchIndex(ctx, tableName); err != nil {
		return fmt.Errorf("failed to ensure search index: %w", err)
	}

	indexTableName := fmt.Sprintf("search_%s", tableName)
	insertSQL := fmt.Sprintf(`
		INSERT INTO %s (id, table_name, search_text, original_data, is_deleted)
		VALUES (?, ?, '', '{}', 1)
	`, indexTableName)

	return r.conn.Exec(ctx, insertSQL, id, tableName)
}

func (r *SearchRepository) GlobalSearchParallel(ctx context.Context, searchTerm string, limit int, cursor string, exactMatch bool, dateFrom *time.Time) (*SearchResult, error) {
	if !r.IsAvailable() {
		return nil, fmt.Errorf("clickhouse not available")
	}

	if limit <= 0 || limit > 100 {
		limit = 20
	}

	searchTerm = strings.ToLower(strings.TrimSpace(searchTerm))
	if searchTerm == "" {
		return nil, fmt.Errorf("search term is required")
	}

	var cursorData SearchCursor
	if cursor != "" {
		decoded, err := base64.URLEncoding.DecodeString(cursor)
		if err == nil {
			json.Unmarshal(decoded, &cursorData)
		}
	}

	// ✅ FIX: Exclude entity tables from parallel search
	tablesQuery := `
		SELECT name
		FROM system.tables
		WHERE database = currentDatabase()
		  AND name LIKE 'search_%'
		  AND name NOT IN ('search_tokens', 'search_entities')
	`

	rows, err := r.conn.Query(ctx, tablesQuery)
	if err != nil {
		return nil, fmt.Errorf("failed to get table list: %w", err)
	}
	defer rows.Close()

	var searchTables []string
	for rows.Next() {
		var tableName string
		if err := rows.Scan(&tableName); err != nil {
			continue
		}
		searchTables = append(searchTables, tableName)
	}

	if len(searchTables) == 0 {
		return &SearchResult{Data: []map[string]interface{}{}, Count: 0}, nil
	}

	log.Printf("[GlobalSearchParallel] Searching %d tables for: '%s'", len(searchTables), searchTerm)

	totalTablesQueried := len(searchTables)

	perTableLimit := ((limit / len(searchTables)) + 1) * 2
	if perTableLimit < 10 {
		perTableLimit = 10
	}

	tokens := strings.Fields(searchTerm)

	type tableResult struct {
		tableName string
		records   []map[string]interface{}
		err       error
	}

	resultsChan := make(chan tableResult, len(searchTables))
	var wg sync.WaitGroup

	for _, tableName := range searchTables {
		wg.Add(1)
		go func(tName string) {
			defer wg.Done()

			var prewhereConditions []string
			prewhereConditions = append(prewhereConditions, "is_deleted = 0")

			if dateFrom != nil && !dateFrom.IsZero() {
				prewhereConditions = append(prewhereConditions,
					fmt.Sprintf("updated_at >= toDateTime('%s')", dateFrom.Format("2006-01-02 15:04:05")))
			}

			if !cursorData.LastUpdatedAt.IsZero() {
				prewhereConditions = append(prewhereConditions, fmt.Sprintf(
					"(updated_at < toDateTime('%s') OR (updated_at = toDateTime('%s') AND id > '%s'))",
					cursorData.LastUpdatedAt.Format("2006-01-02 15:04:05"),
					cursorData.LastUpdatedAt.Format("2006-01-02 15:04:05"),
					cursorData.LastID,
				))
			}

			var whereConditions []string
			if exactMatch {
				for _, token := range tokens {
					whereConditions = append(whereConditions, fmt.Sprintf(
						"(position(search_text, ' %s ') > 0 OR position(search_text, '%s ') = 1 OR position(search_text, ' %s') > 0)",
						token, token, token,
					))
				}
			} else {
				for _, token := range tokens {
					whereConditions = append(whereConditions, fmt.Sprintf("hasToken(search_text, '%s')", token))
				}
			}

			prewhereClause := strings.Join(prewhereConditions, " AND ")
			whereClause := strings.Join(whereConditions, " AND ")

			query := fmt.Sprintf(`
				SELECT id, table_name, original_data, updated_at
				FROM %s
				PREWHERE %s
				WHERE %s
				ORDER BY updated_at DESC, id ASC
				LIMIT %d
			`, tName, prewhereClause, whereClause, perTableLimit)

			tableRows, err := r.conn.Query(ctx, query)
			if err != nil {
				log.Printf("[GlobalSearchParallel] ❌ %s error: %v", tName, err)
				resultsChan <- tableResult{tableName: tName, err: err}
				return
			}
			defer tableRows.Close()

			var records []map[string]interface{}
			for tableRows.Next() {
				var id, sourceTable, originalData string
				var updatedAt time.Time

				if err := tableRows.Scan(&id, &sourceTable, &originalData, &updatedAt); err != nil {
					continue
				}

				var record map[string]interface{}
				if err := json.Unmarshal([]byte(originalData), &record); err != nil {
					record = map[string]interface{}{"id": id}
				}

				record["_source_table"] = sourceTable
				record["_clickhouse_table"] = tName
				record["id"] = id
				record["_updated_at"] = updatedAt
				records = append(records, record)
			}

			log.Printf("[GlobalSearchParallel] ✅ %s: %d results", tName, len(records))
			resultsChan <- tableResult{tableName: tName, records: records}
		}(tableName)
	}

	go func() {
		wg.Wait()
		close(resultsChan)
	}()

	var allResults []map[string]interface{}
	successCount := 0
	for result := range resultsChan {
		if result.err != nil {
			continue
		}
		successCount++
		allResults = append(allResults, result.records...)
	}

	sort.Slice(allResults, func(i, j int) bool {
		ti, _ := allResults[i]["_updated_at"].(time.Time)
		tj, _ := allResults[j]["_updated_at"].(time.Time)
		if ti.Equal(tj) {
			idI, _ := allResults[i]["id"].(string)
			idJ, _ := allResults[j]["id"].(string)
			return idI < idJ
		}
		return ti.After(tj)
	})

	hasMore := len(allResults) > limit
	if len(allResults) > limit {
		allResults = allResults[:limit]
	}

	var nextCursor string
	if hasMore && len(allResults) > 0 {
		lastResult := allResults[len(allResults)-1]
		lastUpdatedAt, _ := lastResult["_updated_at"].(time.Time)
		lastID, _ := lastResult["id"].(string)

		cursorJSON, _ := json.Marshal(SearchCursor{
			LastID:        lastID,
			LastUpdatedAt: lastUpdatedAt,
		})
		nextCursor = base64.URLEncoding.EncodeToString(cursorJSON)
	}

	log.Printf("[GlobalSearchParallel] 🎯 %d results from %d tables (hasMore: %v)", len(allResults), successCount, hasMore)

	return &SearchResult{
		Data:          allResults,
		Count:         len(allResults),
		HasMore:       hasMore,
		NextCursor:    nextCursor,
		TablesQueried: totalTablesQueried,
	}, nil
}

func (r *SearchRepository) GlobalSearch(ctx context.Context, searchTerm string, limit int, exactMatch bool) (*SearchResult, error) {
	return r.GlobalSearchParallel(ctx, searchTerm, limit, "", exactMatch, nil)
}

func (r *SearchRepository) MultiColumnSearch(ctx context.Context, params SearchParams) (*SearchResult, error) {
	if !r.IsAvailable() {
		return nil, fmt.Errorf("clickhouse not available")
	}

	if err := r.EnsureSearchIndex(ctx, params.TableName); err != nil {
		return nil, fmt.Errorf("failed to ensure search index: %w", err)
	}

	searchTerm := strings.ToLower(strings.TrimSpace(params.SearchTerm))
	if searchTerm == "" {
		return nil, fmt.Errorf("search term is required")
	}

	if params.Limit <= 0 || params.Limit > 100 {
		params.Limit = 20
	}

	tokens := strings.Fields(searchTerm)
	var searchConditions []string
	for _, token := range tokens {
		searchConditions = append(searchConditions, fmt.Sprintf("hasToken(search_text, '%s')", token))
	}

	whereClause := strings.Join(searchConditions, " AND ")
	indexTableName := fmt.Sprintf("search_%s", params.TableName)

	query := fmt.Sprintf(`
		SELECT id, table_name, original_data
		FROM %s
		PREWHERE is_deleted = 0 AND (%s)
		ORDER BY updated_at DESC
		LIMIT %d
	`, indexTableName, whereClause, params.Limit+1)

	log.Printf("[MultiColumnSearch] %s: '%s' (limit: %d)", params.TableName, searchTerm, params.Limit)

	rows, err := r.conn.Query(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("search query failed: %w", err)
	}
	defer rows.Close()

	var results []map[string]interface{}
	for rows.Next() {
		var id, tableName, originalData string
		if err := rows.Scan(&id, &tableName, &originalData); err != nil {
			continue
		}

		var record map[string]interface{}
		if err := json.Unmarshal([]byte(originalData), &record); err != nil {
			record = map[string]interface{}{"id": id}
		}

		results = append(results, record)
		if len(results) >= params.Limit+1 {
			break
		}
	}

	hasMore := len(results) > params.Limit
	if hasMore {
		results = results[:params.Limit]
	}

	var nextCursor string
	if hasMore && len(results) > 0 {
		lastRecord := results[len(results)-1]
		cursorData := map[string]interface{}{
			"last_id": lastRecord["id"],
			"offset":  params.Limit,
		}
		cursorJSON, _ := json.Marshal(cursorData)
		nextCursor = base64.URLEncoding.EncodeToString(cursorJSON)
	}

	log.Printf("[MultiColumnSearch] %s: Returned %d results", params.TableName, len(results))

	return &SearchResult{
		Data:          results,
		NextCursor:    nextCursor,
		HasMore:       hasMore,
		Count:         len(results),
		SearchColumns: params.SearchColumns,
	}, nil
}

func (r *SearchRepository) SearchWithCursor(
	ctx context.Context,
	tableName, searchTerm string,
	searchColumns []string,
	limit int,
	cursorStr string,
) ([]map[string]interface{}, string, bool, error) {
	if !r.IsAvailable() {
		return nil, "", false, fmt.Errorf("ClickHouse not available")
	}

	searchTable := fmt.Sprintf("search_%s", tableName)

	var cursor *SearchCursor
	if cursorStr != "" {
		decoded, err := base64.StdEncoding.DecodeString(cursorStr)
		if err == nil {
			var c SearchCursor
			if json.Unmarshal(decoded, &c) == nil {
				cursor = &c
			}
		}
	}

	var conditions []string
	escapedTerm := strings.ReplaceAll(searchTerm, "'", "\\'")
	for _, col := range searchColumns {
		conditions = append(conditions, fmt.Sprintf("hasToken(`%s`, '%s')", col, escapedTerm))
	}

	whereClause := fmt.Sprintf("is_deleted = 0 AND (%s)", strings.Join(conditions, " OR "))

	if cursor != nil {
		whereClause += fmt.Sprintf(
			" AND (updated_at < toDateTime('%s') OR (updated_at = toDateTime('%s') AND id > '%s'))",
			cursor.LastUpdatedAt.Format("2006-01-02 15:04:05"),
			cursor.LastUpdatedAt.Format("2006-01-02 15:04:05"),
			cursor.LastID,
		)
	}

	searchQuery := fmt.Sprintf(`
		SELECT id, original_data, updated_at
		FROM %s
		PREWHERE %s
		ORDER BY updated_at DESC, id ASC
		LIMIT %d
	`, searchTable, whereClause, limit+1)

	rows, err := r.conn.Query(ctx, searchQuery)
	if err != nil {
		return nil, "", false, fmt.Errorf("search query failed: %w", err)
	}
	defer rows.Close()

	results := make([]map[string]interface{}, 0, limit)
	var lastID string
	var lastUpdatedAt time.Time

	for rows.Next() {
		var id, originalData string
		var updatedAt time.Time

		if err := rows.Scan(&id, &originalData, &updatedAt); err != nil {
			log.Printf("[SearchWithCursor] Failed to scan row: %v", err)
			continue
		}

		var record map[string]interface{}
		if err := json.Unmarshal([]byte(originalData), &record); err != nil {
			record = map[string]interface{}{"id": id}
		}

		results = append(results, record)
		lastID = id
		lastUpdatedAt = updatedAt

		if len(results) >= limit+1 {
			break
		}
	}

	hasMore := len(results) > limit
	if hasMore {
		results = results[:limit]
	}

	var nextCursor string
	if hasMore && len(results) > 0 {
		cursorData := SearchCursor{
			LastID:        lastID,
			LastUpdatedAt: lastUpdatedAt,
		}
		cursorJSON, _ := json.Marshal(cursorData)
		nextCursor = base64.StdEncoding.EncodeToString(cursorJSON)
	}

	log.Printf("[SearchWithCursor] %s: Returning %d results, hasMore=%v", tableName, len(results), hasMore)
	return results, nextCursor, hasMore, nil
}

func (r *SearchRepository) GetSyncStats(ctx context.Context, tableName string) (*SyncStats, error) {
	if !r.IsAvailable() {
		return nil, fmt.Errorf("clickhouse not available")
	}

	indexTableName := fmt.Sprintf("search_%s", tableName)
	query := fmt.Sprintf(`
		SELECT
			COUNT(*) as record_count,
			MAX(synced_at) as last_sync
		FROM %s
		WHERE is_deleted = 0
	`, indexTableName)

	var recordCount int
	var lastSync time.Time

	row := r.conn.QueryRow(ctx, query)
	if err := row.Scan(&recordCount, &lastSync); err != nil {
		return nil, fmt.Errorf("failed to get sync stats: %w", err)
	}

	return &SyncStats{
		TableName:   tableName,
		RecordCount: recordCount,
		LastSyncAt:  lastSync,
	}, nil
}

func (r *SearchRepository) GetEntityRepository() *EntityRepository {
	return r.entityRepo
}

func (r *SearchRepository) InitializeEntitySearch(ctx context.Context) error {
	if r.entityRepo == nil {
		return fmt.Errorf("entity repository not initialized")
	}
	return r.entityRepo.Initialize(ctx)
}

func (r *SearchRepository) SearchHybrid(ctx context.Context, searchTerm string, limit int, cursor string) (*SearchResult, error) {
	if !r.IsAvailable() {
		return nil, fmt.Errorf("clickhouse not available")
	}

	words := strings.Fields(searchTerm)
	if len(words) >= 2 && r.entityRepo != nil && r.entityRepo.IsEnabled() {
		log.Printf("[Hybrid] Trying entity search for: %s", searchTerm)

		results, totalMatches, err := r.entityRepo.SearchThreeLayer(ctx, searchTerm, limit)
		if err == nil && len(results) > 0 {
			log.Printf("[Hybrid] Entity search succeeded: %d results", len(results))
			return &SearchResult{
				Data:    results,
				Count:   len(results),
				HasMore: totalMatches > limit,
			}, nil
		}

		log.Printf("[Hybrid] Entity search returned no results, falling back to parallel")
	}

	log.Printf("[Hybrid] Using parallel search for: %s", searchTerm)
	return r.GlobalSearchParallel(ctx, searchTerm, limit, cursor, false, nil)
}

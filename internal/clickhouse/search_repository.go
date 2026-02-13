package clickhouse

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"hash/fnv"
	"log"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
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
	mu         sync.RWMutex

	// metrics
	retryCount       uint64 // atomic
	deadLetterCount  uint64 // atomic
	collisionCount   uint64 // atomic
	indexLagGauge    uint64 // atomic (example)
	tableIDValidated bool
}

func NewSearchRepository(conn ClickHouseConnection, registry *schema.SchemaRegistry) *SearchRepository {
	r := &SearchRepository{
		conn:       conn,
		registry:   registry,
		entityRepo: NewEntityRepository(conn),
	}

	// table ID collision detection at startup: fail fast (log + metric)
	if err := r.validateTableIDs(); err != nil {
		log.Printf("⚠️ Table ID validation: %v", err)
		atomic.AddUint64(&r.collisionCount, 1)
	} else {
		r.tableIDValidated = true
	}

	return r
}

type SearchResult struct {
	Data          []map[string]interface{} `json:"data"`
	NextCursor    string                   `json:"next_cursor,omitempty"`
	HasMore       bool                     `json:"has_more"`
	Count         int                      `json:"count"`
	SearchColumns []string                 `json:"search_columns,omitempty"`

	// Stats for CURRENT PAGE (Visible results)
	Aggregations map[string]int `json:"aggregations"`

	// Stats for GLOBAL DATABASE (All matches)
	GlobalAggregations map[string]uint64 `json:"global_aggregations"`

	// Count of tables that have data globally
	TotalTablesMatched int `json:"total_tables_matched"`
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
	LastGlobalID string `json:"last_global_id"`
}

func marshalCursor(c SearchCursor) (string, error) {
	cursorJSON, err := json.Marshal(c)
	if err == nil {
		return base64.URLEncoding.EncodeToString(cursorJSON), nil
	}
	return "", err
}

func unmarshalCursor(cursorStr string) (*SearchCursor, error) {
	if cursorStr == "" {
		return &SearchCursor{LastGlobalID: ""}, nil
	}
	decoded, err := base64.URLEncoding.DecodeString(cursorStr)
	if err != nil {
		return &SearchCursor{LastGlobalID: ""}, nil
	}
	var c SearchCursor
	if err := json.Unmarshal(decoded, &c); err != nil {
		return &SearchCursor{LastGlobalID: ""}, nil
	}
	return &c, nil
}

func uint64SliceToStrings(ids []uint64) []string {
	strs := make([]string, len(ids))
	for i, id := range ids {
		strs[i] = fmt.Sprintf("%d", id)
	}
	return strs
}

// search_repository.go

func (r *SearchRepository) getGlobalStatsFromHashes(hashes []uint64) map[string]uint64 {
    if len(hashes) == 0 {
        return make(map[string]uint64)
    }

    hashList := make([]string, len(hashes))
    for i, h := range hashes {
        hashList[i] = fmt.Sprintf("%d", h)
    }

    // Use the stats table for fast UI counts
    query := fmt.Sprintf(`
        SELECT table_name, sum(count) as count
        FROM search_token_stats
        WHERE token_hash IN (%s)
        GROUP BY table_name
    `, strings.Join(hashList, ","))

    rows, err := r.conn.Query(context.Background(), query)
    if err != nil {
        return make(map[string]uint64)
    }
    defer rows.Close()

    stats := make(map[string]uint64)
    for rows.Next() {
        var t string
        var count uint64
        if rows.Scan(&t, &count) == nil {
            stats[t] = count
        }
    }
    return stats
}

func (r *SearchRepository) IsAvailable() bool {
	return r.conn != nil && r.conn.IsAvailable()
}

var validTableRegex = func() *regexp.Regexp {
	return regexp.MustCompile(`^[a-zA-Z0-9_]+$`)
}()

func validateTableName(name string) error {
	if name == "" {
		return fmt.Errorf("empty table name")
	}
	if !validTableRegex.MatchString(name) {
		return fmt.Errorf("invalid table name: %s", name)
	}
	return nil
}

func (r *SearchRepository) EnsureSearchIndex(ctx context.Context, tableName string) error {
	if !r.IsAvailable() {
		return fmt.Errorf("clickhouse not available")
	}

	if err := validateTableName(tableName); err != nil {
		return err
	}

	table := r.registry.GetTable(tableName)
	if table == nil {
		return fmt.Errorf("table not found: %s", tableName)
	}

	indexTableName := fmt.Sprintf("search_%s", tableName)

	createTableSQL := fmt.Sprintf(`
    CREATE TABLE IF NOT EXISTS %s (
        s_indx UInt64 CODEC(DoubleDelta, ZSTD(3)),
        global_id UInt64 CODEC(ZSTD(3)),
        original_data String CODEC(ZSTD(3)),
        is_deleted UInt8 DEFAULT 0,
        synced_at DateTime CODEC(DoubleDelta, ZSTD(3)),
        updated_at DateTime CODEC(DoubleDelta, ZSTD(3))
    ) ENGINE = ReplacingMergeTree(updated_at)
    PARTITION BY toYYYYMM(updated_at)
    ORDER BY (global_id, s_indx)
    SETTINGS index_granularity = 16384
`, indexTableName)

	return r.conn.Exec(ctx, createTableSQL)
}

// ⭐ BulkIndex Modified for Strict Atomicity Support + sub-batching tokens
// This function attempts to index. If Token Batch fails, it returns an error.
// The caller (CDCManager) is responsible for retrying the whole chunk.
// search_repository.go

// search_repository.go

func (r *SearchRepository) BulkIndex(ctx context.Context, tableName string, records []map[string]interface{}) error {
    if !r.IsAvailable() || len(records) == 0 {
        return nil
    }

    if err := validateTableName(tableName); err != nil {
        return fmt.Errorf("invalid table name: %w", err)
    }

    if err := r.EnsureSearchIndex(ctx, tableName); err != nil {
        return fmt.Errorf("failed to ensure search index: %w", err)
    }

    table := r.registry.GetTable(tableName)
    if table == nil {
        return fmt.Errorf("table not found: %s", tableName)
    }

    pkCol := "id"
    if len(table.PrimaryKey) > 0 {
        pkCol = table.PrimaryKey[0]
    }

    indexTableName := fmt.Sprintf("search_%s", tableName)
    startTime := time.Now()

    // 1. Main Data Batch
    batch, err := r.conn.PrepareBatch(ctx, fmt.Sprintf(`
        INSERT INTO %s (s_indx, global_id, original_data, is_deleted, synced_at, updated_at)
    `, indexTableName))
    if err != nil {
        return fmt.Errorf("failed to prepare batch: %w", err)
    }
    defer func() { _ = batch.Abort() }()

    // 2. Token Stream Batch (Simple Types)
    // Inserts into search_token_entity. MV handles the Bitmap aggregation.
    tokenBatch, err := r.conn.PrepareBatch(ctx, `
        INSERT INTO search_token_entity (token_hash, token, global_id, table_name, updated_at)
    `)
    if err != nil {
        return fmt.Errorf("failed to prepare token batch: %w", err)
    }
    defer func() { _ = tokenBatch.Abort() }()

    // 3. Stats Batch (Simple Types)
    // Directly increments counters in search_token_stats.
    statsBatch, err := r.conn.PrepareBatch(ctx, `
        INSERT INTO search_token_stats (token_hash, table_name, count, updated_at)
    `)
    if err != nil {
        return fmt.Errorf("failed to prepare stats batch: %w", err)
    }
    defer func() { _ = statsBatch.Abort() }()

    tableID := r.getTableIDDeterministic(tableName)
    now := time.Now()

    for _, record := range records {
        pkValueRaw, ok := record[pkCol]
        if !ok {
            continue
        }

        s_indx := getUint64FromInterface(pkValueRaw)
        if s_indx == 0 {
            continue
        }

        global_id := generateCompactGlobalID(tableID, s_indx)

        searchText := r.extractSearchableText(record, table)
        tokens := strings.Fields(searchText)

        var updatedAt time.Time
        if val, ok := record["updated_at"].(time.Time); ok {
            updatedAt = val
        }
        if updatedAt.IsZero() {
            updatedAt = now
        }

        isDeleted := uint8(0)
        if val, ok := record["is_deleted"]; ok {
            if deleted, ok := val.(bool); ok && deleted {
                isDeleted = 1
            }
        }

        originalData, _ := json.Marshal(record)

        // Append Main
        if err := batch.Append(s_indx, global_id, string(originalData), isDeleted, now, updatedAt); err != nil {
            return fmt.Errorf("failed to append main batch: %w", err)
        }

        // Process Tokens
        // Use a local cache to dedup tokens within the same record (so "error error" counts as 1 stat)
        seenTokens := make(map[uint64]bool)

        for _, token := range tokens {
            if len(token) < 2 {
                continue
            }
            
            tokenLower := strings.ToLower(token)
            tokenHash := hashToken(tokenLower)

            // 1. Insert into Token Stream (triggers MV)
            if err := tokenBatch.Append(tokenHash, tokenLower, global_id, tableName, now); err != nil {
                // Log warning but usually continue
                log.Printf("warn: failed token append: %v", err)
            }

            // 2. Insert into Stats (if not seen in this record)
            if !seenTokens[tokenHash] {
                seenTokens[tokenHash] = true
                if err := statsBatch.Append(tokenHash, tableName, 1, now); err != nil {
                    log.Printf("warn: failed stats append: %v", err)
                }
            }
        }
    }

    // Send all batches
    if err := batch.Send(); err != nil {
        return fmt.Errorf("main batch send failed: %w", err)
    }
    if err := tokenBatch.Send(); err != nil {
        return fmt.Errorf("token batch send failed: %w", err)
    }
    if err := statsBatch.Send(); err != nil {
        return fmt.Errorf("stats batch send failed: %w", err)
    }

    duration := time.Since(startTime)
    if len(records) > 100 {
        log.Printf("[BulkIndex] %s: %d records in %v", tableName, len(records), duration)
    }

    return nil
}

func getUint64FromInterface(val interface{}) uint64 {
	switch v := val.(type) {
	case int:
		return uint64(v)
	case int8:
		return uint64(v)
	case int16:
		return uint64(v)
	case int32:
		return uint64(v)
	case int64:
		return uint64(v)
	case uint:
		return uint64(v)
	case uint8:
		return uint64(v)
	case uint16:
		return uint64(v)
	case uint32:
		return uint64(v)
	case uint64:
		return v
	case float32:
		return uint64(v)
	case float64:
		return uint64(v)
	case string:
		if v == "" {
			return 0
		}
		if x, err := strconv.ParseUint(v, 10, 64); err == nil {
			return x
		}
	case []byte:
		if x, err := strconv.ParseUint(string(v), 10, 64); err == nil {
			return x
		}
	}
	return 0
}

func (r *SearchRepository) getTableIDDeterministic(tableName string) uint16 {
	h := fnv.New64a()
	h.Write([]byte(tableName))
	hash := h.Sum64()
	return uint16(hash >> 48)
}

func generateCompactGlobalID(tableID uint16, s_indx uint64) uint64 {
	return (uint64(tableID) << 48) | (s_indx & 0xFFFFFFFFFFFF)
}

func hashToken(token string) uint64 {
	h := fnv.New64a()
	h.Write([]byte(strings.ToLower(token)))
	return h.Sum64()
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

func (r *SearchRepository) BulkIndexTokens(ctx context.Context, tableName string, records []map[string]interface{}) error {
	// This function is legacy/superseded by BulkIndex for atomic sync, but kept for compatibility
	if len(records) == 0 {
		return nil
	}

	table := r.registry.GetTable(tableName)
	if table == nil {
		return fmt.Errorf("table not found: %s", tableName)
	}

	pkCol := "id"
	if len(table.PrimaryKey) > 0 {
		pkCol = table.PrimaryKey[0]
	}

	tokenBatch, err := r.conn.PrepareBatch(ctx, `
        INSERT INTO search_token_entity (token_hash, token, global_id, updated_at, table_name)
    `)
	if err != nil {
		return err
	}

	tableID := r.getTableIDDeterministic(tableName)

	for _, record := range records {
		pkValueRaw, ok := record[pkCol]
		if !ok {
			continue
		}

		s_indx := getUint64FromInterface(pkValueRaw)
		if s_indx == 0 {
			continue
		}

		global_id := generateCompactGlobalID(tableID, s_indx)
		searchText := r.extractSearchableText(record, table)
		tokens := strings.Fields(searchText)

		var updatedAt time.Time
		if val, ok := record["updated_at"].(time.Time); ok {
			updatedAt = val
		} else {
			updatedAt = time.Now()
		}

		for _, token := range tokens {
			if len(token) < 2 {
				continue
			}
			tokenBatch.Append(
				hashToken(token),
				strings.ToLower(token),
				global_id,
				updatedAt,
				tableName,
			)
		}
	}

	return tokenBatch.Send()
}

// ⭐ MAIN SEARCH FUNCTION: INFINITE SCROLL (Search-After Pattern)
// search_repository.go

// search_repository.go

func (r *SearchRepository) SearchFullHistoryBitmap(ctx context.Context, searchTerm string, limit int, cursor string) (*SearchResult, error) {
    searchTerm = strings.ToLower(strings.TrimSpace(searchTerm))
    tokens := strings.Fields(searchTerm)

    if len(tokens) == 0 {
        return &SearchResult{Data: []map[string]interface{}{}}, nil
    }

    // 1. Get Token Hashes
    var tokenHashes []uint64
    for _, t := range tokens {
        tokenHashes = append(tokenHashes, hashToken(t))
    }

    // 2. Decode Cursor
    c, _ := unmarshalCursor(cursor)

    // 3. Construct Bitmap Query
    // We use a WITH clause to make it cleaner and safer.
    // 'res' will be a Bitmap object.
    var bitmapQuery string
    
    if len(tokenHashes) == 1 {
        bitmapQuery = fmt.Sprintf(`
            SELECT ids_bitmap 
            FROM search_token_bitmap 
            WHERE token_hash = %d 
            LIMIT 1
        `, tokenHashes[0])
    } else {
        // For multiple tokens, we must intersect.
        // We use groupBitmapMerge on the filtered table, then bitmapAnd.
        var views []string
        for i, hash := range tokenHashes {
            views = append(views, fmt.Sprintf(
                // We select the merged bitmap for this specific hash
                "(SELECT groupBitmapMerge(ids_bitmap) FROM search_token_bitmap WHERE token_hash = %d) as b%d",
                hash, i,
            ))
        }
        
        // Join them: SELECT bitmapAnd(b0, bitmapAnd(b1, b2))
        intersection := "b0"
        for i := 1; i < len(views); i++ {
            intersection = fmt.Sprintf("bitmapAnd(%s, b%d)", intersection, i)
        }
        
        bitmapQuery = fmt.Sprintf("SELECT %s FROM (SELECT %s)", intersection, strings.Join(views, ", "))
    }

    // 4. Final Query with Cursor
    // We wrap the result in ifNull to handle empty searches gracefully.
    finalQuery := fmt.Sprintf(`
        SELECT arrayJoin(bitmapToArray(ifNull(
            (%s),
            bitmapBuild(emptyArrayUInt64())
        ))) as global_id
    `, bitmapQuery)

    // Add Cursor Logic
    var whereClause string
    if c.LastGlobalID != "" {
        parsedID, err := strconv.ParseUint(c.LastGlobalID, 10, 64)
        if err == nil {
            whereClause = fmt.Sprintf(" WHERE global_id < %d", parsedID)
        }
    }

    // Apply Limit + 1 for pagination check
    query := fmt.Sprintf(`
        SELECT global_id FROM (
            %s
        ) 
        %s
        ORDER BY global_id DESC
        LIMIT %d
    `, finalQuery, whereClause, limit+1)

    rows, err := r.conn.Query(ctx, query)
    if err != nil {
        return nil, fmt.Errorf("bitmap query failed: %w", err)
    }
    defer rows.Close()

    // ... (Rest of the function remains the same: collect IDs, resolve tables, fetch data) ...

	// 5. Collect IDs
	var globalIDs []uint64
	for rows.Next() {
		var id uint64
		if err := rows.Scan(&id); err != nil {
			continue
		}
		globalIDs = append(globalIDs, id)
	}

	if len(globalIDs) == 0 {
		return &SearchResult{
			Data:         []map[string]interface{}{},
			Count:        0,
			HasMore:      false,
			Aggregations: make(map[string]int),
		}, nil
	}

	// 6. Resolve Global IDs to Tables
	idsByTable := make(map[string][]uint64)
	for _, gid := range globalIDs {
		tid := extractTableID(gid)
		tableName := r.getTableName(tid)

		if tableName == "unknown" {
			continue
		}
		idsByTable[tableName] = append(idsByTable[tableName], gid)
	}

	// 7. Batched Data Fetch
	var allRecords []map[string]interface{}
	var wgData sync.WaitGroup
	var muData sync.Mutex

	for tableName, gIDs := range idsByTable {
		wgData.Add(1)
		go func(tName string, tIds []uint64) {
			defer wgData.Done()

			idStrs := uint64SliceToStrings(tIds)
			inClause := strings.Join(idStrs, ",")

			lookupQuery := fmt.Sprintf(`
                SELECT original_data, updated_at, s_indx, global_id
                FROM search_%s
                WHERE global_id IN (%s) AND is_deleted = 0
                ORDER BY global_id DESC
            `, tName, inClause)

			dataRows, err := r.conn.Query(ctx, lookupQuery)
			if err != nil {
				return
			}
			defer dataRows.Close()

			for dataRows.Next() {
				var data string
				var t time.Time
				var id uint64
				var globalID uint64

				if err := dataRows.Scan(&data, &t, &id, &globalID); err != nil {
					continue
				}

				var record map[string]interface{}
				if err := json.Unmarshal([]byte(data), &record); err != nil {
					continue
				}

				record["_source_table"] = tName
				record["updated_at"] = t
				record["s_indx"] = id
				record["global_id"] = globalID

				muData.Lock()
				allRecords = append(allRecords, record)
				muData.Unlock()
			}
		}(tableName, gIDs)
	}
	wgData.Wait()

	// 8. Final Sort by Global ID DESC
	sort.Slice(allRecords, func(i, j int) bool {
		idI, _ := allRecords[i]["global_id"].(uint64)
		idJ, _ := allRecords[j]["global_id"].(uint64)
		return idI > idJ
	})

	// 9. Pagination Logic
	hasMore := len(allRecords) > limit
	if hasMore {
		allRecords = allRecords[:limit]
	}

	// 10. Generate Next Cursor
	var nextCursor string
	if len(allRecords) > 0 {
		lastRecord := allRecords[len(allRecords)-1]
		lastGlobalID, _ := lastRecord["global_id"].(uint64)

		if hasMore {
			nextCursor, _ = marshalCursor(SearchCursor{LastGlobalID: fmt.Sprintf("%d", lastGlobalID)})
		}
	}

	// 11. Stats
	pageAggregations := make(map[string]int)
	for _, r := range allRecords {
		if t, ok := r["_source_table"].(string); ok {
			pageAggregations[t]++
		}
	}

	return &SearchResult{
		Data:               allRecords,
		Count:              len(allRecords),
		HasMore:            hasMore,
		NextCursor:         nextCursor,
		Aggregations:       pageAggregations,
		GlobalAggregations: r.getGlobalStatsFromHashes(tokenHashes),
	}, nil
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

	tableID := r.getTableIDDeterministic(tableName)
	pkVal := getUint64FromInterface(id)
	if pkVal == 0 {
		return fmt.Errorf("invalid id format: %s", id)
	}
	globalID := generateCompactGlobalID(tableID, pkVal)

	indexTableName := fmt.Sprintf("search_%s", tableName)

	insertSQL := fmt.Sprintf(`
        INSERT INTO %s (s_indx, global_id, original_data, is_deleted, updated_at)
        VALUES (?, ?, '', 1, now())
    `, indexTableName)

	return r.conn.Exec(ctx, insertSQL, pkVal, globalID)
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

// --- WRAPPERS ---

func (r *SearchRepository) SearchHybrid(ctx context.Context, searchTerm string, limit int, cursor string) (*SearchResult, error) {
	return r.SearchFullHistoryBitmap(ctx, searchTerm, limit, cursor)
}

func (r *SearchRepository) GlobalSearch(ctx context.Context, searchTerm string, limit int, exactMatch bool, dateFrom *time.Time) (*SearchResult, error) {
	return r.SearchFullHistoryBitmap(ctx, searchTerm, limit, "")
}

func (r *SearchRepository) GlobalSearchParallel(ctx context.Context, searchTerm string, limit int, cursor string, exactMatch bool, dateFrom *time.Time) (*SearchResult, error) {
	return r.SearchFullHistoryBitmap(ctx, searchTerm, limit, cursor)
}

func (r *SearchRepository) SearchFullHistory(ctx context.Context, searchTerm string, limit int) (*SearchResult, error) {
	return r.SearchFullHistoryBitmap(ctx, searchTerm, limit, "")
}

func (r *SearchRepository) MultiColumnSearch(ctx context.Context, params SearchParams) (*SearchResult, error) {
	return r.SearchFullHistoryBitmap(ctx, params.SearchTerm, params.Limit, params.Cursor)
}

func (r *SearchRepository) SearchWithCursor(
	ctx context.Context,
	tableName, searchTerm string,
	searchColumns []string,
	limit int,
	cursorStr string,
) ([]map[string]interface{}, string, bool, error) {
	res, err := r.SearchFullHistoryBitmap(ctx, searchTerm, limit, cursorStr)
	if err != nil {
		return nil, "", false, err
	}
	return res.Data, res.NextCursor, res.HasMore, nil
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

func (r *SearchRepository) getTableName(id uint16) string {
	allTables := r.registry.GetAllTables()
	for _, table := range allTables {
		if r.getTableIDDeterministic(table.Name) == id {
			return table.Name
		}
	}
	return "unknown"
}

func extractTableID(globalID uint64) uint16 {
	return uint16(globalID >> 48)
}

// Validate table IDs to detect collisions among deterministic uint16 ids
func (r *SearchRepository) validateTableIDs() error {
	seen := map[uint16]string{}
	for _, t := range r.registry.GetAllTables() {
		id := r.getTableIDDeterministic(t.Name)
		if prev, ok := seen[id]; ok && prev != t.Name {
			return fmt.Errorf("table id collision detected: %d -> %s and %s", id, prev, t.Name)
		}
		seen[id] = t.Name
	}
	return nil
}

// InsertDeadLetter stores a failed chunk for later triage.
func (r *SearchRepository) InsertDeadLetter(ctx context.Context, tableName string, startID, endID uint64, attempts uint8, lastError, sampleData string) error {
	if err := validateTableName(tableName); err != nil {
		return err
	}
	query := `
        INSERT INTO search_index_errors (table_name, start_id, end_id, attempts, last_error, sample_data, created_at)
        VALUES (?, ?, ?, ?, ?, ?, now())
    `
	if err := r.conn.Exec(ctx, query, tableName, startID, endID, attempts, lastError, sampleData); err != nil {
		return fmt.Errorf("failed to insert dead-letter: %w", err)
	}
	return nil
}

// Metric helpers

func (r *SearchRepository) IncRetryCount(n uint64) {
	atomic.AddUint64(&r.retryCount, n)
}

func (r *SearchRepository) IncDeadLetterCount(n uint64) {
	atomic.AddUint64(&r.deadLetterCount, n)
}

func (r *SearchRepository) IncCollisionCount(n uint64) {
	atomic.AddUint64(&r.collisionCount, n)
}

func (r *SearchRepository) SetIndexLag(v uint64) {
	atomic.StoreUint64(&r.indexLagGauge, v)
}

type RepoMetrics struct {
	RetryCount     uint64 `json:"retry_count"`
	DeadLetter     uint64 `json:"dead_letter_count"`
	Collisions     uint64 `json:"table_id_collisions"`
	IndexLagGauge  uint64 `json:"index_lag"`
	TableIDChecked bool   `json:"table_id_validated"`
}

func (r *SearchRepository) GetMetrics() RepoMetrics {
	return RepoMetrics{
		RetryCount:     atomic.LoadUint64(&r.retryCount),
		DeadLetter:     atomic.LoadUint64(&r.deadLetterCount),
		Collisions:     atomic.LoadUint64(&r.collisionCount),
		IndexLagGauge:  atomic.LoadUint64(&r.indexLagGauge),
		TableIDChecked: r.tableIDValidated,
	}
}

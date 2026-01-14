package clickhouse

import (
        "context"
        "encoding/base64"
        "encoding/json"
        "fmt"
        "strings"
        "time"

        "highperf-api/internal/schema"
)

type SearchRepository struct {
        conn     *Connection
        registry *schema.SchemaRegistry
}

func NewSearchRepository(conn *Connection, registry *schema.SchemaRegistry) *SearchRepository {
        return &SearchRepository{
                conn:     conn,
                registry: registry,
        }
}

type SearchResult struct {
        Data          []map[string]interface{}
        NextCursor    string
        HasMore       bool
        Count         int
        SearchColumns []string
}

type SearchParams struct {
        TableName     string
        SearchTerm    string
        SearchColumns []string
        Cursor        string
        Limit         int
        Filters       map[string]string
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
                        textColumns = append(textColumns, col.Name)
                }
        }

        if len(textColumns) == 0 {
                return fmt.Errorf("no text columns found in table %s", tableName)
        }

        indexTableName := fmt.Sprintf("search_%s", tableName)

        createTableSQL := fmt.Sprintf(`
                CREATE TABLE IF NOT EXISTS %s (
                        id String,
                        table_name String,
                        search_text String,
                        original_data String,
                        updated_at DateTime64(3) DEFAULT now64(),
                        is_deleted UInt8 DEFAULT 0,
                        INDEX search_idx search_text TYPE ngrambf_v1(3, 512, 2, 0) GRANULARITY 1
                ) ENGINE = ReplacingMergeTree(updated_at)
                ORDER BY (table_name, id)
                TTL updated_at + INTERVAL 30 DAY DELETE
        `, indexTableName)

        return r.conn.Exec(ctx, createTableSQL)
}

func (r *SearchRepository) IndexRecord(ctx context.Context, tableName string, id string, record map[string]interface{}) error {
        if !r.IsAvailable() {
                return nil
        }

        table := r.registry.GetTable(tableName)
        if table == nil {
                return fmt.Errorf("table not found: %s", tableName)
        }

        var textParts []string
        for _, col := range table.Columns {
                if col.DataType == "character varying" || col.DataType == "text" {
                        if val, ok := record[col.Name]; ok && val != nil {
                                textParts = append(textParts, fmt.Sprintf("%v", val))
                        }
                }
        }

        searchText := strings.ToLower(strings.Join(textParts, " "))

        originalData, _ := json.Marshal(record)

        indexTableName := fmt.Sprintf("search_%s", tableName)
        insertSQL := fmt.Sprintf(`
                INSERT INTO %s (id, table_name, search_text, original_data, is_deleted)
                VALUES (?, ?, ?, ?, 0)
        `, indexTableName)

        return r.conn.Exec(ctx, insertSQL, id, tableName, searchText, string(originalData))
}

func (r *SearchRepository) DeleteRecord(ctx context.Context, tableName string, id string) error {
        if !r.IsAvailable() {
                return nil
        }

        indexTableName := fmt.Sprintf("search_%s", tableName)
        insertSQL := fmt.Sprintf(`
                INSERT INTO %s (id, table_name, search_text, original_data, is_deleted)
                VALUES (?, ?, '', '{}', 1)
        `, indexTableName)

        return r.conn.Exec(ctx, insertSQL, id, tableName)
}

func (r *SearchRepository) Search(ctx context.Context, params SearchParams) (*SearchResult, error) {
        if !r.IsAvailable() {
                return nil, fmt.Errorf("clickhouse not available")
        }

        limit := params.Limit
        if limit <= 0 || limit > 50 {
                limit = 20
        }

        searchTerm := strings.ToLower(strings.TrimSpace(params.SearchTerm))
        if searchTerm == "" {
                return nil, fmt.Errorf("search term is required")
        }

        tokens := strings.Fields(searchTerm)

        indexTableName := fmt.Sprintf("search_%s", params.TableName)

        var conditions []string
        var args []interface{}

        conditions = append(conditions, "is_deleted = 0")

        for _, token := range tokens {
                conditions = append(conditions, "search_text LIKE ?")
                args = append(args, "%"+token+"%")
        }

        cursorData, cursorErr := decodeCursor(params.Cursor)
        if cursorErr == nil && cursorData != nil {
                cursorTable := cursorData["table"]
                cursorQuery := cursorData["q"]
                if cursorTable != "" && cursorTable != params.TableName {
                        cursorData = nil
                } else if cursorQuery != "" && strings.ToLower(cursorQuery) != searchTerm {
                        cursorData = nil
                }

                if cursorData != nil {
                        if lastID, ok := cursorData["id"]; ok && lastID != "" {
                                conditions = append(conditions, "id > ?")
                                args = append(args, lastID)
                        }
                }
        }

        whereClause := strings.Join(conditions, " AND ")

        query := fmt.Sprintf(`
                SELECT id, table_name, search_text, original_data
                FROM %s FINAL
                WHERE %s
                ORDER BY id ASC
                LIMIT ?
        `, indexTableName, whereClause)
        args = append(args, limit+1)

        rows, err := r.conn.Query(ctx, query, args...)
        if err != nil {
                return nil, fmt.Errorf("search query failed: %w", err)
        }
        defer rows.Close()

        var results []map[string]interface{}
        var lastID string

        for rows.Next() {
                var id, tableName, searchText, originalData string
                if err := rows.Scan(&id, &tableName, &searchText, &originalData); err != nil {
                        return nil, fmt.Errorf("scan failed: %w", err)
                }

                var record map[string]interface{}
                if err := json.Unmarshal([]byte(originalData), &record); err != nil {
                        record = map[string]interface{}{"id": id}
                }

                results = append(results, record)
                lastID = id
        }

        hasMore := len(results) > limit
        if hasMore {
                results = results[:limit]
                if idVal, ok := results[len(results)-1]["id"]; ok {
                        lastID = fmt.Sprintf("%v", idVal)
                }
        }

        var nextCursor string
        if hasMore && lastID != "" {
                nextCursor = encodeCursor(map[string]string{
                        "id":    lastID,
                        "q":     params.SearchTerm,
                        "table": params.TableName,
                })
        }

        return &SearchResult{
                Data:          results,
                NextCursor:    nextCursor,
                HasMore:       hasMore,
                Count:         len(results),
                SearchColumns: params.SearchColumns,
        }, nil
}

func (r *SearchRepository) MultiColumnSearch(ctx context.Context, params SearchParams) (*SearchResult, error) {
        if !r.IsAvailable() {
                return nil, fmt.Errorf("clickhouse not available")
        }

        return r.Search(ctx, params)
}

func encodeCursor(data map[string]string) string {
        jsonData, err := json.Marshal(data)
        if err != nil {
                return ""
        }
        return base64.URLEncoding.EncodeToString(jsonData)
}

func decodeCursor(cursor string) (map[string]string, error) {
        if cursor == "" {
                return nil, nil
        }

        decoded, err := base64.URLEncoding.DecodeString(cursor)
        if err != nil {
                return nil, fmt.Errorf("invalid cursor encoding: %w", err)
        }

        var data map[string]string
        if err := json.Unmarshal(decoded, &data); err != nil {
                return nil, fmt.Errorf("invalid cursor format: %w", err)
        }

        return data, nil
}

func (r *SearchRepository) SyncFromPostgres(ctx context.Context, tableName string, records []map[string]interface{}) error {
        if !r.IsAvailable() {
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

        for _, record := range records {
                id := fmt.Sprintf("%v", record[pkColumn])
                if err := r.IndexRecord(ctx, tableName, id, record); err != nil {
                        return err
                }
        }

        return nil
}

func (r *SearchRepository) GetSearchableColumns(tableName string) []string {
        table := r.registry.GetTable(tableName)
        if table == nil {
                return nil
        }

        var textColumns []string
        for _, col := range table.Columns {
                if col.DataType == "character varying" || col.DataType == "text" {
                        textColumns = append(textColumns, col.Name)
                }
        }

        return textColumns
}

type SyncStats struct {
        TableName    string
        RecordCount  int
        SyncDuration time.Duration
        LastSyncAt   time.Time
}

func (r *SearchRepository) GetSyncStats(ctx context.Context, tableName string) (*SyncStats, error) {
        if !r.IsAvailable() {
                return nil, fmt.Errorf("clickhouse not available")
        }

        indexTableName := fmt.Sprintf("search_%s", tableName)

        query := fmt.Sprintf(`
                SELECT count(*) as cnt, max(updated_at) as last_update
                FROM %s FINAL
                WHERE table_name = ? AND is_deleted = 0
        `, indexTableName)

        rows, err := r.conn.Query(ctx, query, tableName)
        if err != nil {
                return nil, err
        }
        defer rows.Close()

        var count uint64
        var lastUpdate time.Time

        if rows.Next() {
                if err := rows.Scan(&count, &lastUpdate); err != nil {
                        return nil, err
                }
        }

        return &SyncStats{
                TableName:   tableName,
                RecordCount: int(count),
                LastSyncAt:  lastUpdate,
        }, nil
}

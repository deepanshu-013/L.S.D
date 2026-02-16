package handlers

import (
	"compress/gzip"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"strconv"
	"strings"
	"time"

	"highperf-api/internal/cache"
	chpkg "highperf-api/internal/clickhouse"
	"highperf-api/internal/database"
	"highperf-api/internal/schema"
)

type DynamicHandler struct {
	repo        *database.DynamicRepository
	registry    *schema.SchemaRegistry
	cache       *cache.MultiLayerCache
	chSearch    *chpkg.SearchRepository
	cdcManager  *chpkg.CDCManager
	maxPageSize int
	defaultSize int
	timeout     time.Duration
}

func NewDynamicHandler(repo *database.DynamicRepository, registry *schema.SchemaRegistry, cache *cache.MultiLayerCache, chSearch *chpkg.SearchRepository, maxPageSize, defaultSize int, timeout time.Duration) *DynamicHandler {
	return &DynamicHandler{
		repo:        repo,
		registry:    registry,
		cache:       cache,
		chSearch:    chSearch,
		maxPageSize: maxPageSize,
		defaultSize: defaultSize,
		timeout:     timeout,
	}
}

func (h *DynamicHandler) SetCDCManager(cdcManager *chpkg.CDCManager) {
	h.cdcManager = cdcManager
}

func (h *DynamicHandler) GetCDCStatus(w http.ResponseWriter, r *http.Request) {
	if h.cdcManager == nil {
		h.writeJSON(w, http.StatusOK, map[string]interface{}{
			"is_running":     false,
			"available":      false,
			"total_tables":   0,
			"table_statuses": map[string]interface{}{},
		})
		return
	}

	status := h.cdcManager.GetStatus()
	h.writeJSON(w, http.StatusOK, status)
}

func (h *DynamicHandler) ListTables(w http.ResponseWriter, r *http.Request) {
	tables := h.registry.GetAllTables()
	clickhouseAvailable := h.chSearch != nil && h.chSearch.IsAvailable()

	tableList := make([]map[string]interface{}, 0, len(tables))
	for _, t := range tables {
		searchableColumns := h.getSearchableColumns(t.Name)
		tableList = append(tableList, map[string]interface{}{
			"name":              t.Name,
			"schema":            t.Schema,
			"columns":           len(t.Columns),
			"primary_key":       t.PrimaryKey,
			"sortable":          h.registry.GetSortableColumns(t.Name),
			"filterable":        h.registry.GetFilterableColumns(t.Name),
			"searchable":        searchableColumns,
			"clickhouse_search": clickhouseAvailable,
		})
	}

	h.writeJSON(w, http.StatusOK, map[string]interface{}{
		"tables": tableList,
		"count":  len(tableList),
	})
}

func (h *DynamicHandler) GetTableSchema(w http.ResponseWriter, r *http.Request) {
	tableName := r.PathValue("table")
	if tableName == "" {
		h.writeError(w, http.StatusBadRequest, "Table name is required")
		return
	}

	table := h.registry.GetTable(tableName)
	if table == nil {
		h.writeError(w, http.StatusNotFound, "Table not found")
		return
	}

	searchableColumns := h.getSearchableColumns(tableName)
	clickhouseAvailable := h.chSearch != nil && h.chSearch.IsAvailable()

	h.writeJSON(w, http.StatusOK, map[string]interface{}{
		"name":              table.Name,
		"schema":            table.Schema,
		"columns":           table.Columns,
		"primary_key":       table.PrimaryKey,
		"indexes":           table.Indexes,
		"sortable":          h.registry.GetSortableColumns(tableName),
		"filterable":        h.registry.GetFilterableColumns(tableName),
		"searchable":        searchableColumns,
		"clickhouse_search": clickhouseAvailable,
	})
}

func (h *DynamicHandler) GetRecords(w http.ResponseWriter, r *http.Request) {
	tableName := r.PathValue("table")
	if tableName == "" {
		h.writeError(w, http.StatusBadRequest, "Table name is required")
		return
	}

	if !h.registry.TableExists(tableName) {
		h.writeError(w, http.StatusNotFound, "Table not found")
		return
	}

	params := h.parseQueryParams(r, tableName)
	ctx, cancel := context.WithTimeout(r.Context(), h.timeout)
	defer cancel()

	if params.Cursor == "" {
		cacheKey := h.cache.GenerateCacheKey(tableName, params.Filters, params.Cursor, params.Limit, params.SortBy, params.SortDir)
		var cachedResult database.DynamicResult
		if hit, _ := h.cache.Get(ctx, cacheKey, &cachedResult); hit {
			h.writeJSONCompressed(w, r, http.StatusOK, map[string]interface{}{
				"data":        cachedResult.Data,
				"next_cursor": cachedResult.NextCursor,
				"has_more":    cachedResult.HasMore,
				"count":       cachedResult.Count,
				"table":       tableName,
				"cached":      true,
			})
			return
		}
	}

	result, err := h.repo.GetRecords(ctx, params)
	if err != nil {
		h.writeError(w, http.StatusInternalServerError, "Failed to fetch records: "+err.Error())
		return
	}

	if params.Cursor == "" {
		cacheKey := h.cache.GenerateCacheKey(tableName, params.Filters, params.Cursor, params.Limit, params.SortBy, params.SortDir)
		h.cache.Set(ctx, cacheKey, result)
	}

	h.writeJSONCompressed(w, r, http.StatusOK, map[string]interface{}{
		"data":        result.Data,
		"next_cursor": result.NextCursor,
		"has_more":    result.HasMore,
		"count":       result.Count,
		"table":       tableName,
	})
}

func (h *DynamicHandler) GetRecordByPK(w http.ResponseWriter, r *http.Request) {
	tableName := r.PathValue("table")
	pkValue := r.PathValue("pk")

	if tableName == "" || pkValue == "" {
		h.writeError(w, http.StatusBadRequest, "Table name and primary key are required")
		return
	}

	if !h.registry.TableExists(tableName) {
		h.writeError(w, http.StatusNotFound, "Table not found")
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), h.timeout)
	defer cancel()

	var pk interface{} = pkValue
	if intVal, err := strconv.ParseInt(pkValue, 10, 64); err == nil {
		pk = intVal
	}

	record, err := h.repo.GetRecordByPK(ctx, tableName, pk)
	if err != nil {
		h.writeError(w, http.StatusInternalServerError, "Failed to fetch record")
		return
	}

	if record == nil {
		h.writeError(w, http.StatusNotFound, "Record not found")
		return
	}

	h.writeJSON(w, http.StatusOK, record)
}

func (h *DynamicHandler) SearchRecords(w http.ResponseWriter, r *http.Request) {
	tableName := r.PathValue("table")
	if tableName == "" {
		h.writeError(w, http.StatusBadRequest, "Table name is required")
		return
	}

	if !h.registry.TableExists(tableName) {
		h.writeError(w, http.StatusNotFound, "Table not found")
		return
	}

	searchTerm := r.URL.Query().Get("q")
	searchColumnsParam := r.URL.Query().Get("columns")
	engineParam := r.URL.Query().Get("engine")

	if searchTerm == "" {
		h.writeError(w, http.StatusBadRequest, "Search term (q) is required")
		return
	}

	if len(searchTerm) < 2 || len(searchTerm) > 100 {
		h.writeError(w, http.StatusBadRequest, "Search term must be 2-100 characters")
		return
	}

	var searchColumns []string
	if searchColumnsParam != "" {
		searchColumns = strings.Split(searchColumnsParam, ",")
		for i := range searchColumns {
			searchColumns[i] = strings.TrimSpace(searchColumns[i])
		}
	} else {
		searchColumns = h.getSearchableColumns(tableName)
	}

	if len(searchColumns) == 0 {
		h.writeError(w, http.StatusBadRequest, "No searchable text columns found")
		return
	}

	params := h.parseQueryParams(r, tableName)
	ctx, cancel := context.WithTimeout(r.Context(), h.timeout)
	defer cancel()

	useClickHouse := engineParam != "postgresql" && h.chSearch != nil && h.chSearch.IsAvailable()

	if useClickHouse {
		lowerSearchColumns := make([]string, len(searchColumns))
		for i, col := range searchColumns {
			lowerSearchColumns[i] = strings.ToLower(col)
		}

		results, nextCursor, hasMore, err := h.chSearch.SearchWithCursor(
			ctx, tableName, searchTerm, lowerSearchColumns, params.Limit, params.Cursor)

		if err == nil {
			h.writeJSONCompressed(w, r, http.StatusOK, map[string]interface{}{
				"data":           results,
				"next_cursor":    nextCursor,
				"has_more":       hasMore,
				"count":          len(results),
				"table":          tableName,
				"search_columns": searchColumns,
				"search_engine":  "clickhouse",
			})
			return
		}

		log.Printf("ClickHouse search failed, falling back to PostgreSQL: %v", err)
	}

	result, err := h.multiColumnPostgresSearch(ctx, tableName, searchTerm, searchColumns, params)
	if err != nil {
		h.writeError(w, http.StatusInternalServerError, "Failed to search records: "+err.Error())
		return
	}

	h.writeJSONCompressed(w, r, http.StatusOK, map[string]interface{}{
		"data":           result.Data,
		"next_cursor":    result.NextCursor,
		"has_more":       result.HasMore,
		"count":          result.Count,
		"table":          tableName,
		"search_columns": searchColumns,
		"search_engine":  "postgresql",
	})
}

func (h *DynamicHandler) SearchOptimized(w http.ResponseWriter, r *http.Request) {
	query := r.URL.Query().Get("q")
	cursor := r.URL.Query().Get("cursor")

	if query == "" {
		h.writeError(w, http.StatusBadRequest, "Query parameter 'q' is required")
		return
	}

	if len(query) < 2 || len(query) > 100 {
		h.writeError(w, http.StatusBadRequest, "Search term must be 2-100 characters")
		return
	}

	limit := 20
	if l := r.URL.Query().Get("limit"); l != "" {
		if parsed, err := strconv.Atoi(l); err == nil && parsed > 0 && parsed <= 100 {
			limit = parsed
		}
	}

	if h.chSearch == nil || !h.chSearch.IsAvailable() {
		h.writeError(w, http.StatusServiceUnavailable, "ClickHouse not available")
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), h.timeout)
	defer cancel()

	// ⭐ FIXED: Added timing
	startTime := time.Now()
	result, err := h.chSearch.SearchFullHistoryBitmap(ctx, query, limit, cursor)

	if err != nil {
		h.writeError(w, http.StatusInternalServerError, "Optimized search failed: "+err.Error())
		return
	}

	duration := time.Since(startTime)

	// Group results by table for the JSON structure
	tableGroups := make(map[string][]map[string]interface{})
	for _, record := range result.Data {
		if sourceTable, ok := record["_source_table"].(string); ok {
			tableGroups[sourceTable] = append(tableGroups[sourceTable], record)
		}
	}

	h.writeJSONCompressed(w, r, http.StatusOK, map[string]interface{}{
		"results":       tableGroups,
		"total_results": result.Count,
		"has_more":      result.HasMore,
		"next_cursor":   result.NextCursor,
		"search_time":   duration.Milliseconds(), // ⭐ FIXED: Actual time in ms
		"query":         query,
		"limit":         limit,
		"search_engine": "clickhouse_optimized_map",
	})
}

// Update GetEntityStats
func (h *DynamicHandler) GetEntityStats(w http.ResponseWriter, r *http.Request) {
	h.writeJSON(w, http.StatusServiceUnavailable, map[string]interface{}{
		"enabled": false,
		"message": "Entity layer is disabled.",
	})
}

func (h *DynamicHandler) ReindexEntities(w http.ResponseWriter, r *http.Request) {
	tableName := r.PathValue("table")
	if tableName == "" {
		h.writeError(w, http.StatusBadRequest, "Table name is required")
		return
	}

	if h.cdcManager == nil {
		h.writeError(w, http.StatusServiceUnavailable, "CDC manager not available")
		return
	}

	go func() {
		if err := h.cdcManager.TriggerTableSync(tableName); err != nil {
			log.Printf("Failed to reindex %s: %v", tableName, err)
		}
	}()

	h.writeJSON(w, http.StatusAccepted, map[string]interface{}{
		"message": fmt.Sprintf("Entity reindexing triggered for %s", tableName),
		"status":  "processing",
	})
}

func (h *DynamicHandler) multiColumnPostgresSearch(ctx context.Context, tableName, searchTerm string, searchColumns []string, params schema.QueryParams) (*database.DynamicResult, error) {
	if len(searchColumns) == 1 {
		return h.repo.SearchRecords(ctx, params, searchColumns[0], searchTerm)
	}
	return h.repo.MultiColumnSearch(ctx, params, searchColumns, searchTerm)
}

func (h *DynamicHandler) GetTableStats(w http.ResponseWriter, r *http.Request) {
	tableName := r.PathValue("table")
	if tableName == "" {
		h.writeError(w, http.StatusBadRequest, "Table name is required")
		return
	}

	if !h.registry.TableExists(tableName) {
		h.writeError(w, http.StatusNotFound, "Table not found")
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), h.timeout)
	defer cancel()

	stats, err := h.repo.GetTableStatsEstimated(ctx, tableName)
	if err != nil {
		h.writeError(w, http.StatusInternalServerError, "Failed to fetch stats: "+err.Error())
		return
	}

	if h.chSearch != nil && h.chSearch.IsAvailable() {
		syncStats, err := h.chSearch.GetSyncStats(ctx, tableName)
		if err == nil && syncStats != nil {
			stats["clickhouse_indexed"] = syncStats.RecordCount
			stats["clickhouse_last_sync"] = syncStats.LastSyncAt
		}
	}

	h.writeJSON(w, http.StatusOK, stats)
}

func (h *DynamicHandler) HealthCheck(w http.ResponseWriter, r *http.Request) {
	tables := h.registry.GetAllTables()
	clickhouseAvailable := false
	if h.chSearch != nil {
		clickhouseAvailable = h.chSearch.IsAvailable()
	}

	h.writeJSON(w, http.StatusOK, map[string]interface{}{
		"status":       "UP",
		"service":      "L.S.D",
		"tables_count": len(tables),
		"clickhouse":   clickhouseAvailable,
		"redis":        h.cache.IsAvailable(),
	})
}

func (h *DynamicHandler) getSearchableColumns(tableName string) []string {
	table := h.registry.GetTable(tableName)
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

func (h *DynamicHandler) parseQueryParams(r *http.Request, tableName string) schema.QueryParams {
	params := schema.QueryParams{
		TableName: tableName,
		Cursor:    r.URL.Query().Get("cursor"),
		SortBy:    r.URL.Query().Get("sort_by"),
		SortDir:   r.URL.Query().Get("sort_dir"),
		Filters:   make(map[string]string),
	}

	if limitStr := r.URL.Query().Get("limit"); limitStr != "" {
		if limit, err := strconv.Atoi(limitStr); err == nil {
			params.Limit = limit
		}
	}

	if params.Limit <= 0 {
		params.Limit = h.defaultSize
	}

	if params.Limit > h.maxPageSize {
		params.Limit = h.maxPageSize
	}

	filterableCols := h.registry.GetFilterableColumns(tableName)
	for _, col := range filterableCols {
		if value := r.URL.Query().Get(col); value != "" {
			params.Filters[col] = value
		}
	}

	return params
}

func (h *DynamicHandler) writeJSON(w http.ResponseWriter, status int, data interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(data)
}

func (h *DynamicHandler) writeJSONCompressed(w http.ResponseWriter, r *http.Request, status int, data interface{}) {
	// 1. Always set Content-Type
	w.Header().Set("Content-Type", "application/json")

	// 2. Check if the client supports Gzip (Browsers always do)
	// We use 'strings.Contains' to be safe against varied header formats
	acceptsGzip := strings.Contains(strings.ToLower(r.Header.Get("Accept-Encoding")), "gzip")

	if acceptsGzip {
		w.Header().Set("Content-Encoding", "gzip")
		w.WriteHeader(status)

		// Create a Gzip writer
		gz := gzip.NewWriter(w)
		defer gz.Close()

		// Encode directly to the Gzip stream
		// This happens in-memory and streams out compressed
		if err := json.NewEncoder(gz).Encode(data); err != nil {
			// Log the error but don't panic, we've already written status code
			log.Printf("Error encoding gzip response: %v", err)
		}
		return
	}

	// 3. Fallback only if client doesn't support Gzip
	h.writeJSON(w, status, data)
}

func (h *DynamicHandler) writeError(w http.ResponseWriter, status int, message string) {
	h.writeJSON(w, status, map[string]string{"error": message})
}

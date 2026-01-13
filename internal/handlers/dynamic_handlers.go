package handlers

import (
	"context"
	"encoding/json"
	"net/http"
	"strconv"
	"time"

	"highperf-api/internal/cache"
	"highperf-api/internal/database"
	"highperf-api/internal/schema"
)

type DynamicHandler struct {
	repo        *database.DynamicRepository
	registry    *schema.SchemaRegistry
	cache       *cache.RedisCache
	maxPageSize int
	defaultSize int
	timeout     time.Duration
}

func NewDynamicHandler(repo *database.DynamicRepository, registry *schema.SchemaRegistry, cache *cache.RedisCache, maxPageSize, defaultSize int, timeout time.Duration) *DynamicHandler {
	return &DynamicHandler{
		repo:        repo,
		registry:    registry,
		cache:       cache,
		maxPageSize: maxPageSize,
		defaultSize: defaultSize,
		timeout:     timeout,
	}
}

func (h *DynamicHandler) ListTables(w http.ResponseWriter, r *http.Request) {
	tables := h.registry.GetAllTables()

	tableList := make([]map[string]interface{}, 0, len(tables))
	for _, t := range tables {
		tableList = append(tableList, map[string]interface{}{
			"name":         t.Name,
			"schema":       t.Schema,
			"columns":      len(t.Columns),
			"primary_key":  t.PrimaryKey,
			"sortable":     h.registry.GetSortableColumns(t.Name),
			"filterable":   h.registry.GetFilterableColumns(t.Name),
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

	h.writeJSON(w, http.StatusOK, map[string]interface{}{
		"name":        table.Name,
		"schema":      table.Schema,
		"columns":     table.Columns,
		"primary_key": table.PrimaryKey,
		"indexes":     table.Indexes,
		"sortable":    h.registry.GetSortableColumns(tableName),
		"filterable":  h.registry.GetFilterableColumns(tableName),
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

	cacheKey := h.cache.GenerateCacheKey(tableName, params.Filters, params.Cursor, params.Limit)
	var cachedResult database.DynamicResult
	if hit, _ := h.cache.Get(ctx, cacheKey, &cachedResult); hit {
		h.writeJSON(w, http.StatusOK, map[string]interface{}{
			"data":        cachedResult.Data,
			"next_cursor": cachedResult.NextCursor,
			"has_more":    cachedResult.HasMore,
			"count":       cachedResult.Count,
			"table":       tableName,
			"cached":      true,
		})
		return
	}

	result, err := h.repo.GetRecords(ctx, params)
	if err != nil {
		h.writeError(w, http.StatusInternalServerError, "Failed to fetch records: "+err.Error())
		return
	}

	h.cache.Set(ctx, cacheKey, result)

	h.writeJSON(w, http.StatusOK, map[string]interface{}{
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
	searchColumn := r.URL.Query().Get("column")

	if searchTerm == "" {
		h.writeError(w, http.StatusBadRequest, "Search term (q) is required")
		return
	}

	if len(searchTerm) < 2 || len(searchTerm) > 100 {
		h.writeError(w, http.StatusBadRequest, "Search term must be 2-100 characters")
		return
	}

	table := h.registry.GetTable(tableName)
	if searchColumn == "" {
		for _, col := range table.Columns {
			if col.DataType == "character varying" || col.DataType == "text" {
				searchColumn = col.Name
				break
			}
		}
	}

	if searchColumn == "" {
		h.writeError(w, http.StatusBadRequest, "No searchable text column found")
		return
	}

	params := h.parseQueryParams(r, tableName)

	ctx, cancel := context.WithTimeout(r.Context(), h.timeout)
	defer cancel()

	result, err := h.repo.SearchRecords(ctx, params, searchColumn, searchTerm)
	if err != nil {
		h.writeError(w, http.StatusInternalServerError, "Failed to search records")
		return
	}

	h.writeJSON(w, http.StatusOK, map[string]interface{}{
		"data":          result.Data,
		"next_cursor":   result.NextCursor,
		"has_more":      result.HasMore,
		"count":         result.Count,
		"table":         tableName,
		"search_column": searchColumn,
	})
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

	stats, err := h.repo.GetTableStats(ctx, tableName)
	if err != nil {
		h.writeError(w, http.StatusInternalServerError, "Failed to fetch stats")
		return
	}

	h.writeJSON(w, http.StatusOK, stats)
}

func (h *DynamicHandler) HealthCheck(w http.ResponseWriter, r *http.Request) {
	tables := h.registry.GetAllTables()
	h.writeJSON(w, http.StatusOK, map[string]interface{}{
		"status":       "healthy",
		"service":      "dynamic-api",
		"tables_count": len(tables),
	})
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

func (h *DynamicHandler) writeError(w http.ResponseWriter, status int, message string) {
	h.writeJSON(w, status, map[string]string{"error": message})
}

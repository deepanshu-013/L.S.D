package database

import (
	"context"
	"fmt"
	"strings"

	"highperf-api/internal/schema"
)

type DynamicRepository struct {
	pool         *Pool
	registry     *schema.SchemaRegistry
	queryBuilder *schema.QueryBuilder
}

func NewDynamicRepository(pool *Pool, registry *schema.SchemaRegistry) *DynamicRepository {
	return &DynamicRepository{
		pool:         pool,
		registry:     registry,
		queryBuilder: schema.NewQueryBuilder(registry),
	}
}

type DynamicResult struct {
	Data       []map[string]interface{}
	NextCursor string
	HasMore    bool
	Count      int
}

func (r *DynamicRepository) GetRecords(ctx context.Context, params schema.QueryParams) (*DynamicResult, error) {
	builtQuery, err := r.queryBuilder.BuildSelectQuery(params)
	if err != nil {
		return nil, err
	}

	rows, err := r.pool.Query(ctx, builtQuery.SQL, builtQuery.Args...)
	if err != nil {
		return nil, fmt.Errorf("query failed: %w", err)
	}
	defer rows.Close()

	var records []map[string]interface{}
	for rows.Next() {
		values, err := rows.Values()
		if err != nil {
			return nil, fmt.Errorf("values failed: %w", err)
		}

		record := make(map[string]interface{})
		for i, col := range builtQuery.Columns {
			record[col] = values[i]
		}
		records = append(records, record)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("rows error: %w", err)
	}

	limit := params.Limit
	if limit <= 0 || limit > 50 {
		limit = 20
	}

	var nextCursor string
	hasMore := len(records) > limit
	if hasMore {
		records = records[:limit]
		lastRecord := records[len(records)-1]
		nextCursor = r.queryBuilder.EncodeCursor(params.TableName, lastRecord, builtQuery.SortColumn)
	}

	return &DynamicResult{
		Data:       records,
		NextCursor: nextCursor,
		HasMore:    hasMore,
		Count:      len(records),
	}, nil
}

func (r *DynamicRepository) GetRecordByPK(ctx context.Context, tableName string, pkValue interface{}) (map[string]interface{}, error) {
	builtQuery, err := r.queryBuilder.BuildGetByPKQuery(tableName, pkValue)
	if err != nil {
		return nil, err
	}

	rows, err := r.pool.Query(ctx, builtQuery.SQL, builtQuery.Args...)
	if err != nil {
		return nil, fmt.Errorf("query failed: %w", err)
	}
	defer rows.Close()

	if !rows.Next() {
		return nil, nil
	}

	values, err := rows.Values()
	if err != nil {
		return nil, fmt.Errorf("values failed: %w", err)
	}

	record := make(map[string]interface{})
	for i, col := range builtQuery.Columns {
		record[col] = values[i]
	}

	return record, nil
}

func (r *DynamicRepository) SearchRecords(ctx context.Context, params schema.QueryParams, searchColumn, searchTerm string) (*DynamicResult, error) {
	builtQuery, err := r.queryBuilder.BuildSearchQuery(params, searchColumn, searchTerm)
	if err != nil {
		return nil, err
	}

	rows, err := r.pool.Query(ctx, builtQuery.SQL, builtQuery.Args...)
	if err != nil {
		return nil, fmt.Errorf("query failed: %w", err)
	}
	defer rows.Close()

	var records []map[string]interface{}
	for rows.Next() {
		values, err := rows.Values()
		if err != nil {
			return nil, fmt.Errorf("values failed: %w", err)
		}

		record := make(map[string]interface{})
		for i, col := range builtQuery.Columns {
			record[col] = values[i]
		}
		records = append(records, record)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("rows error: %w", err)
	}

	limit := params.Limit
	if limit <= 0 || limit > 50 {
		limit = 20
	}

	var nextCursor string
	hasMore := len(records) > limit
	if hasMore {
		records = records[:limit]
		lastRecord := records[len(records)-1]
		nextCursor = r.queryBuilder.EncodeCursor(params.TableName, lastRecord, builtQuery.SortColumn)
	}

	return &DynamicResult{
		Data:       records,
		NextCursor: nextCursor,
		HasMore:    hasMore,
		Count:      len(records),
	}, nil
}

func (r *DynamicRepository) MultiColumnSearch(ctx context.Context, params schema.QueryParams, searchColumns []string, searchTerm string) (*DynamicResult, error) {
	table := r.registry.GetTable(params.TableName)
	if table == nil {
		return nil, fmt.Errorf("table not found: %s", params.TableName)
	}

	var columns []string
	for _, col := range table.Columns {
		columns = append(columns, col.Name)
	}

	limit := params.Limit
	if limit <= 0 || limit > 50 {
		limit = 20
	}

	var searchConditions []string
	var args []interface{}
	argIndex := 1

	for _, col := range searchColumns {
		searchConditions = append(searchConditions, fmt.Sprintf("%s ILIKE $%d", col, argIndex))
		args = append(args, "%"+searchTerm+"%")
		argIndex++
	}

	whereClause := "WHERE (" + strings.Join(searchConditions, " OR ") + ")"

	cursor, err := r.queryBuilder.DecodeCursor(params.Cursor)
	if err != nil {
		return nil, err
	}

	if cursor != nil && len(cursor.Values) > 0 {
		cv := cursor.Values[len(cursor.Values)-1]
		parsedVal, err := parseValueFromCursor(cv.Value, cv.Type)
		if err != nil {
			return nil, err
		}
		whereClause += fmt.Sprintf(" AND %s > $%d", cv.Column, argIndex)
		args = append(args, parsedVal)
		argIndex++
	}

	pkOrder := table.PrimaryKey[0]

	query := fmt.Sprintf(`
		SELECT %s
		FROM %s
		%s
		ORDER BY %s ASC
		LIMIT $%d
	`, strings.Join(columns, ", "), params.TableName, whereClause, pkOrder, argIndex)
	args = append(args, limit+1)

	rows, err := r.pool.Query(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("query failed: %w", err)
	}
	defer rows.Close()

	var records []map[string]interface{}
	for rows.Next() {
		values, err := rows.Values()
		if err != nil {
			return nil, fmt.Errorf("values failed: %w", err)
		}

		record := make(map[string]interface{})
		for i, col := range columns {
			record[col] = values[i]
		}
		records = append(records, record)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("rows error: %w", err)
	}

	var nextCursor string
	hasMore := len(records) > limit
	if hasMore {
		records = records[:limit]
		lastRecord := records[len(records)-1]
		nextCursor = r.queryBuilder.EncodeCursor(params.TableName, lastRecord, pkOrder)
	}

	return &DynamicResult{
		Data:       records,
		NextCursor: nextCursor,
		HasMore:    hasMore,
		Count:      len(records),
	}, nil
}

func parseValueFromCursor(val interface{}, dataType string) (interface{}, error) {
	return val, nil
}

func (r *DynamicRepository) GetTableStats(ctx context.Context, tableName string) (map[string]interface{}, error) {
	table := r.registry.GetTable(tableName)
	if table == nil {
		return nil, fmt.Errorf("table not found: %s", tableName)
	}

	query := fmt.Sprintf(`
		SELECT 
			COUNT(*) as total_count
		FROM %s
	`, tableName)

	var totalCount int64
	err := r.pool.QueryRow(ctx, query).Scan(&totalCount)
	if err != nil {
		return nil, fmt.Errorf("query failed: %w", err)
	}

	return map[string]interface{}{
		"table_name":   tableName,
		"total_count":  totalCount,
		"column_count": len(table.Columns),
		"columns":      table.Columns,
		"primary_key":  table.PrimaryKey,
		"indexes":      table.Indexes,
	}, nil
}

func (r *DynamicRepository) GetTableStatsEstimated(ctx context.Context, tableName string) (map[string]interface{}, error) {
	table := r.registry.GetTable(tableName)
	if table == nil {
		return nil, fmt.Errorf("table not found: %s", tableName)
	}

	query := `
		SELECT reltuples::bigint as estimated_count
		FROM pg_class c
		JOIN pg_namespace n ON n.oid = c.relnamespace
		WHERE c.relname = $1
		AND n.nspname NOT IN ('pg_catalog', 'information_schema')
		LIMIT 1
	`

	var estimatedCount int64
	err := r.pool.QueryRow(ctx, query, tableName).Scan(&estimatedCount)
	if err != nil {
		return r.GetTableStats(ctx, tableName)
	}

	if estimatedCount < 0 {
		estimatedCount = 0
	}

	return map[string]interface{}{
		"table_name":      tableName,
		"estimated_count": estimatedCount,
		"count_type":      "estimated",
		"column_count":    len(table.Columns),
		"columns":         table.Columns,
		"primary_key":     table.PrimaryKey,
		"indexes":         table.Indexes,
	}, nil
}

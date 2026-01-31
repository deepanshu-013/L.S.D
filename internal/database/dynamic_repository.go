package database

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"

	"highperf-api/internal/schema"

	"github.com/jackc/pgx/v5/pgxpool"
)

type DynamicRepository struct {
	pool     *pgxpool.Pool
	registry *schema.SchemaRegistry
}

func NewDynamicRepository(pool *pgxpool.Pool, registry *schema.SchemaRegistry) *DynamicRepository {
	return &DynamicRepository{
		pool:     pool,
		registry: registry,
	}
}

type DynamicResult struct {
	Data       []map[string]interface{} `json:"data"`
	NextCursor string                   `json:"next_cursor,omitempty"`
	HasMore    bool                     `json:"has_more"`
	Count      int                      `json:"count"`
}

// CursorData represents decoded cursor information
type CursorData struct {
	LastRecord map[string]interface{} `json:"last_record"`
	Offset     int                    `json:"offset"`
}

func (r *DynamicRepository) GetRecords(ctx context.Context, params schema.QueryParams) (*DynamicResult, error) {
	// Decode cursor if present
	var cursorData *CursorData
	if params.Cursor != "" {
		decoded, err := base64.URLEncoding.DecodeString(params.Cursor)
		if err == nil {
			var cd CursorData
			if json.Unmarshal(decoded, &cd) == nil {
				cursorData = &cd
			}
		}
	}

	query, args := r.buildQuery(params, cursorData, false)

	rows, err := r.pool.Query(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("query failed: %w", err)
	}
	defer rows.Close()

	var records []map[string]interface{}
	for rows.Next() {
		values, err := rows.Values()
		if err != nil {
			continue
		}

		fieldDescriptions := rows.FieldDescriptions()
		record := make(map[string]interface{})
		for i, field := range fieldDescriptions {
			if i < len(values) {
				record[string(field.Name)] = values[i]
			}
		}
		records = append(records, record)

		if len(records) >= params.Limit+1 {
			break
		}
	}

	hasMore := len(records) > params.Limit
	if hasMore {
		records = records[:params.Limit]
	}

	var nextCursor string
	if hasMore && len(records) > 0 {
		lastRecord := records[len(records)-1]
		newOffset := params.Limit
		if cursorData != nil {
			newOffset += cursorData.Offset
		}
		cursorData := CursorData{
			LastRecord: lastRecord,
			Offset:     newOffset,
		}
		cursorJSON, _ := json.Marshal(cursorData)
		nextCursor = base64.URLEncoding.EncodeToString(cursorJSON)
	}

	return &DynamicResult{
		Data:       records,
		NextCursor: nextCursor,
		HasMore:    hasMore,
		Count:      len(records),
	}, nil
}

func (r *DynamicRepository) GetRecordByPK(ctx context.Context, tableName string, pk interface{}) (map[string]interface{}, error) {
	table := r.registry.GetTable(tableName)
	if table == nil || len(table.PrimaryKey) == 0 {
		return nil, fmt.Errorf("table not found or no primary key")
	}

	pkColumn := table.PrimaryKey[0]
	query := fmt.Sprintf("SELECT * FROM %s WHERE %s = $1 LIMIT 1", tableName, pkColumn)

	rows, err := r.pool.Query(ctx, query, pk)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	if !rows.Next() {
		return nil, nil
	}

	values, err := rows.Values()
	if err != nil {
		return nil, err
	}

	fieldDescriptions := rows.FieldDescriptions()
	record := make(map[string]interface{})
	for i, field := range fieldDescriptions {
		if i < len(values) {
			record[string(field.Name)] = values[i]
		}
	}

	return record, nil
}

func (r *DynamicRepository) SearchRecords(ctx context.Context, params schema.QueryParams, searchColumn, searchTerm string) (*DynamicResult, error) {
	params.Filters[searchColumn] = "%" + searchTerm + "%"
	return r.GetRecords(ctx, params)
}

func (r *DynamicRepository) MultiColumnSearch(ctx context.Context, params schema.QueryParams, searchColumns []string, searchTerm string) (*DynamicResult, error) {
	query, args := r.buildMultiColumnSearchQuery(params, searchColumns, searchTerm)

	rows, err := r.pool.Query(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("multi-column search failed: %w", err)
	}
	defer rows.Close()

	var records []map[string]interface{}
	for rows.Next() {
		values, err := rows.Values()
		if err != nil {
			continue
		}

		fieldDescriptions := rows.FieldDescriptions()
		record := make(map[string]interface{})
		for i, field := range fieldDescriptions {
			if i < len(values) {
				record[string(field.Name)] = values[i]
			}
		}
		records = append(records, record)

		if len(records) >= params.Limit+1 {
			break
		}
	}

	hasMore := len(records) > params.Limit
	if hasMore {
		records = records[:params.Limit]
	}

	return &DynamicResult{
		Data:    records,
		HasMore: hasMore,
		Count:   len(records),
	}, nil
}

func (r *DynamicRepository) GetTableStatsEstimated(ctx context.Context, tableName string) (map[string]interface{}, error) {
	query := `
        SELECT 
            schemaname,
            relname,
            n_live_tup as estimated_rows
        FROM pg_stat_user_tables
        WHERE relname = $1
    `

	var schema, name string
	var estimatedRows int64

	err := r.pool.QueryRow(ctx, query, tableName).Scan(&schema, &name, &estimatedRows)
	if err != nil {
		return nil, err
	}

	return map[string]interface{}{
		"table":          name,
		"schema":         schema,
		"estimated_rows": estimatedRows,
	}, nil
}

func (r *DynamicRepository) buildQuery(params schema.QueryParams, cursorData *CursorData, countOnly bool) (string, []interface{}) {
	var args []interface{}
	argPos := 1
	selectClause := "*"
	if countOnly {
		selectClause = "COUNT(*)"
	}

	query := fmt.Sprintf("SELECT %s FROM %s WHERE 1=1", selectClause, params.TableName)

	// Apply filters
	for col, val := range params.Filters {
		query += fmt.Sprintf(" AND %s = $%d", col, argPos)
		args = append(args, val)
		argPos++
	}

	// ✅ FIXED: Cursor-based pagination that works with custom sorting
	if cursorData != nil && cursorData.LastRecord != nil && !countOnly {
		table := r.registry.GetTable(params.TableName)
		if table != nil && len(table.PrimaryKey) > 0 {
			pkColumn := table.PrimaryKey[0]

			// If custom sorting is used
			if params.SortBy != "" && r.registry.IsColumnSortable(params.TableName, params.SortBy) {
				sortColumn := params.SortBy
				sortDir := "ASC"
				if params.SortDir == "desc" {
					sortDir = "DESC"
				}

				// Get last sort value and last PK from cursor
				if lastSortValue, sortExists := cursorData.LastRecord[sortColumn]; sortExists {
					if lastPK, pkExists := cursorData.LastRecord[pkColumn]; pkExists {
						// Composite cursor condition for custom sort column + PK tiebreaker
						if sortDir == "DESC" {
							query += fmt.Sprintf(" AND (%s < $%d OR (%s = $%d AND %s < $%d))",
								sortColumn, argPos, sortColumn, argPos, pkColumn, argPos+1)
						} else {
							query += fmt.Sprintf(" AND (%s > $%d OR (%s = $%d AND %s > $%d))",
								sortColumn, argPos, sortColumn, argPos, pkColumn, argPos+1)
						}
						args = append(args, lastSortValue, lastPK)
						argPos += 2
					}
				}
			} else {
				// Default: sort by PK only
				if lastID, ok := cursorData.LastRecord[pkColumn]; ok {
					query += fmt.Sprintf(" AND %s > $%d", pkColumn, argPos)
					args = append(args, lastID)
					argPos++
				}
			}
		}
	}

	if !countOnly {
		// Apply sorting
		if params.SortBy != "" && r.registry.IsColumnSortable(params.TableName, params.SortBy) {
			sortDir := "ASC"
			if params.SortDir == "desc" {
				sortDir = "DESC"
			}

			// Always add PK as tiebreaker for consistent pagination
			table := r.registry.GetTable(params.TableName)
			if table != nil && len(table.PrimaryKey) > 0 {
				pkColumn := table.PrimaryKey[0]
				if params.SortBy != pkColumn {
					query += fmt.Sprintf(" ORDER BY %s %s, %s %s", params.SortBy, sortDir, pkColumn, sortDir)
				} else {
					query += fmt.Sprintf(" ORDER BY %s %s", params.SortBy, sortDir)
				}
			} else {
				query += fmt.Sprintf(" ORDER BY %s %s", params.SortBy, sortDir)
			}
		} else {
			// Default sort by primary key for consistent pagination
			table := r.registry.GetTable(params.TableName)
			if table != nil && len(table.PrimaryKey) > 0 {
				query += fmt.Sprintf(" ORDER BY %s ASC", table.PrimaryKey[0])
			}
		}

		query += fmt.Sprintf(" LIMIT $%d", argPos)
		args = append(args, params.Limit+1)
	}

	return query, args
}

func (r *DynamicRepository) buildMultiColumnSearchQuery(params schema.QueryParams, searchColumns []string, searchTerm string) (string, []interface{}) {
	var args []interface{}
	argPos := 1
	query := fmt.Sprintf("SELECT * FROM %s WHERE ", params.TableName)
	var searchConditions []string
	for _, col := range searchColumns {
		searchConditions = append(searchConditions, fmt.Sprintf("%s ILIKE $%d", col, argPos))
		args = append(args, "%"+searchTerm+"%")
		argPos++
	}

	query += "(" + fmt.Sprintf("%s", searchConditions[0])
	for i := 1; i < len(searchConditions); i++ {
		query += " OR " + searchConditions[i]
	}
	query += ")"

	for col, val := range params.Filters {
		query += fmt.Sprintf(" AND %s = $%d", col, argPos)
		args = append(args, val)
		argPos++
	}

	// ✅ FIXED: Apply sorting with PK tiebreaker
	table := r.registry.GetTable(params.TableName)
	if params.SortBy != "" && r.registry.IsColumnSortable(params.TableName, params.SortBy) {
		sortDir := "ASC"
		if params.SortDir == "desc" {
			sortDir = "DESC"
		}

		// Add PK as tiebreaker
		if table != nil && len(table.PrimaryKey) > 0 {
			pkColumn := table.PrimaryKey[0]
			if params.SortBy != pkColumn {
				query += fmt.Sprintf(" ORDER BY %s %s, %s %s", params.SortBy, sortDir, pkColumn, sortDir)
			} else {
				query += fmt.Sprintf(" ORDER BY %s %s", params.SortBy, sortDir)
			}
		} else {
			query += fmt.Sprintf(" ORDER BY %s %s", params.SortBy, sortDir)
		}
	} else if table != nil && len(table.PrimaryKey) > 0 {
		query += fmt.Sprintf(" ORDER BY %s ASC", table.PrimaryKey[0])
	}

	query += fmt.Sprintf(" LIMIT $%d", argPos)
	args = append(args, params.Limit+1)
	return query, args
}

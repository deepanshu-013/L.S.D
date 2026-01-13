package schema

import (
	"context"
	"fmt"
	"sync"

	"github.com/jackc/pgx/v5/pgxpool"
)

type ColumnInfo struct {
	Name       string `json:"name"`
	DataType   string `json:"data_type"`
	IsNullable bool   `json:"is_nullable"`
	IsPrimary  bool   `json:"is_primary"`
	IsIndexed  bool   `json:"is_indexed"`
	OrdinalPos int    `json:"ordinal_position"`
}

type TableInfo struct {
	Name       string       `json:"name"`
	Schema     string       `json:"schema"`
	Columns    []ColumnInfo `json:"columns"`
	PrimaryKey []string     `json:"primary_key"`
	Indexes    []string     `json:"indexed_columns"`
}

type SchemaRegistry struct {
	tables map[string]*TableInfo
	mu     sync.RWMutex
	pool   *pgxpool.Pool
}

func NewSchemaRegistry(pool *pgxpool.Pool) *SchemaRegistry {
	return &SchemaRegistry{
		tables: make(map[string]*TableInfo),
		pool:   pool,
	}
}

func (r *SchemaRegistry) LoadSchema(ctx context.Context) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	tables, err := r.discoverTables(ctx)
	if err != nil {
		return fmt.Errorf("failed to discover tables: %w", err)
	}

	for _, table := range tables {
		columns, err := r.discoverColumns(ctx, table.Schema, table.Name)
		if err != nil {
			return fmt.Errorf("failed to discover columns for %s: %w", table.Name, err)
		}
		table.Columns = columns

		primaryKey, err := r.discoverPrimaryKey(ctx, table.Schema, table.Name)
		if err != nil {
			return fmt.Errorf("failed to discover primary key for %s: %w", table.Name, err)
		}
		table.PrimaryKey = primaryKey

		indexes, err := r.discoverIndexedColumns(ctx, table.Schema, table.Name)
		if err != nil {
			return fmt.Errorf("failed to discover indexes for %s: %w", table.Name, err)
		}
		table.Indexes = indexes

		for i := range table.Columns {
			for _, pk := range table.PrimaryKey {
				if table.Columns[i].Name == pk {
					table.Columns[i].IsPrimary = true
				}
			}
			for _, idx := range table.Indexes {
				if table.Columns[i].Name == idx {
					table.Columns[i].IsIndexed = true
				}
			}
		}

		r.tables[table.Name] = table
	}

	return nil
}

func (r *SchemaRegistry) discoverTables(ctx context.Context) ([]*TableInfo, error) {
	query := `
		SELECT table_name, table_schema
		FROM information_schema.tables
		WHERE table_schema NOT IN ('pg_catalog', 'information_schema')
		AND table_type = 'BASE TABLE'
		ORDER BY table_name
	`

	rows, err := r.pool.Query(ctx, query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var tables []*TableInfo
	for rows.Next() {
		var table TableInfo
		if err := rows.Scan(&table.Name, &table.Schema); err != nil {
			return nil, err
		}
		tables = append(tables, &table)
	}

	return tables, rows.Err()
}

func (r *SchemaRegistry) discoverColumns(ctx context.Context, schema, tableName string) ([]ColumnInfo, error) {
	query := `
		SELECT 
			column_name,
			data_type,
			is_nullable = 'YES' as is_nullable,
			ordinal_position
		FROM information_schema.columns
		WHERE table_schema = $1 AND table_name = $2
		ORDER BY ordinal_position
	`

	rows, err := r.pool.Query(ctx, query, schema, tableName)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var columns []ColumnInfo
	for rows.Next() {
		var col ColumnInfo
		if err := rows.Scan(&col.Name, &col.DataType, &col.IsNullable, &col.OrdinalPos); err != nil {
			return nil, err
		}
		columns = append(columns, col)
	}

	return columns, rows.Err()
}

func (r *SchemaRegistry) discoverPrimaryKey(ctx context.Context, schema, tableName string) ([]string, error) {
	query := `
		SELECT a.attname
		FROM pg_index i
		JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)
		JOIN pg_class c ON c.oid = i.indrelid
		JOIN pg_namespace n ON n.oid = c.relnamespace
		WHERE i.indisprimary
		AND n.nspname = $1
		AND c.relname = $2
		ORDER BY array_position(i.indkey, a.attnum)
	`

	rows, err := r.pool.Query(ctx, query, schema, tableName)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var pkColumns []string
	for rows.Next() {
		var colName string
		if err := rows.Scan(&colName); err != nil {
			return nil, err
		}
		pkColumns = append(pkColumns, colName)
	}

	return pkColumns, rows.Err()
}

func (r *SchemaRegistry) discoverIndexedColumns(ctx context.Context, schema, tableName string) ([]string, error) {
	query := `
		SELECT DISTINCT a.attname
		FROM pg_index i
		JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)
		JOIN pg_class c ON c.oid = i.indrelid
		JOIN pg_namespace n ON n.oid = c.relnamespace
		WHERE n.nspname = $1
		AND c.relname = $2
		ORDER BY a.attname
	`

	rows, err := r.pool.Query(ctx, query, schema, tableName)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var indexedCols []string
	for rows.Next() {
		var colName string
		if err := rows.Scan(&colName); err != nil {
			return nil, err
		}
		indexedCols = append(indexedCols, colName)
	}

	return indexedCols, rows.Err()
}

func (r *SchemaRegistry) GetTable(name string) *TableInfo {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.tables[name]
}

func (r *SchemaRegistry) GetAllTables() []*TableInfo {
	r.mu.RLock()
	defer r.mu.RUnlock()

	tables := make([]*TableInfo, 0, len(r.tables))
	for _, t := range r.tables {
		tables = append(tables, t)
	}
	return tables
}

func (r *SchemaRegistry) TableExists(name string) bool {
	r.mu.RLock()
	defer r.mu.RUnlock()
	_, exists := r.tables[name]
	return exists
}

func (r *SchemaRegistry) GetSortableColumns(tableName string) []string {
	table := r.GetTable(tableName)
	if table == nil {
		return nil
	}

	var sortable []string
	for _, col := range table.Columns {
		if col.IsIndexed || col.IsPrimary {
			sortable = append(sortable, col.Name)
		}
	}
	return sortable
}

func (r *SchemaRegistry) GetFilterableColumns(tableName string) []string {
	return r.GetSortableColumns(tableName)
}

func (r *SchemaRegistry) IsColumnSortable(tableName, columnName string) bool {
	for _, col := range r.GetSortableColumns(tableName) {
		if col == columnName {
			return true
		}
	}
	return false
}

func (r *SchemaRegistry) GetColumnType(tableName, columnName string) string {
	table := r.GetTable(tableName)
	if table == nil {
		return ""
	}

	for _, col := range table.Columns {
		if col.Name == columnName {
			return col.DataType
		}
	}
	return ""
}

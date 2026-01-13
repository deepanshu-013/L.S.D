package database

import (
        "context"
        "fmt"
        "strings"
        "time"

        "github.com/jackc/pgx/v5"
        "highperf-api/internal/models"
        "highperf-api/internal/pagination"
)

var allowedSortColumns = map[string]bool{
        "id":         true,
        "created_at": true,
        "updated_at": true,
        "name":       true,
        "category":   true,
}

var allowedFilterColumns = map[string]bool{
        "category": true,
        "status":   true,
        "name":     true,
}

type Repository struct {
        pool *Pool
}

func NewRepository(pool *Pool) *Repository {
        return &Repository{pool: pool}
}

func (r *Repository) GetRecords(ctx context.Context, params models.QueryParams) ([]models.Record, string, error) {
        sortBy := "id"
        if params.SortBy != "" && allowedSortColumns[params.SortBy] {
                sortBy = params.SortBy
        }

        sortDir := "ASC"
        if strings.ToUpper(params.SortDir) == "DESC" {
                sortDir = "DESC"
        }

        limit := params.Limit
        if limit <= 0 || limit > 50 {
                limit = 20
        }

        var whereConditions []string
        var args []interface{}
        argIndex := 1

        cursor, err := pagination.DecodeCursor(params.Cursor)
        if err != nil {
                return nil, "", fmt.Errorf("invalid cursor: %w", err)
        }

        if cursor != nil {
                var sortValue interface{}
                switch sortBy {
                case "created_at", "updated_at":
                        parsedTime, err := pagination.ParseSortValueAsTime(cursor.SortValue)
                        if err != nil {
                                return nil, "", fmt.Errorf("invalid timestamp in cursor: %w", err)
                        }
                        sortValue = parsedTime
                case "id":
                        parsedID, err := pagination.ParseSortValueAsInt64(cursor.SortValue)
                        if err != nil {
                                sortValue = cursor.ID
                        } else {
                                sortValue = parsedID
                        }
                default:
                        sortValue = cursor.SortValue
                }

                if sortDir == "ASC" {
                        if sortBy == "id" {
                                whereConditions = append(whereConditions, fmt.Sprintf("id > $%d", argIndex))
                                args = append(args, cursor.ID)
                                argIndex++
                        } else {
                                whereConditions = append(whereConditions, fmt.Sprintf("(%s > $%d OR (%s = $%d AND id > $%d))", sortBy, argIndex, sortBy, argIndex, argIndex+1))
                                args = append(args, sortValue, cursor.ID)
                                argIndex += 2
                        }
                } else {
                        if sortBy == "id" {
                                whereConditions = append(whereConditions, fmt.Sprintf("id < $%d", argIndex))
                                args = append(args, cursor.ID)
                                argIndex++
                        } else {
                                whereConditions = append(whereConditions, fmt.Sprintf("(%s < $%d OR (%s = $%d AND id < $%d))", sortBy, argIndex, sortBy, argIndex, argIndex+1))
                                args = append(args, sortValue, cursor.ID)
                                argIndex += 2
                        }
                }
        }

        for key, value := range params.Filters {
                if allowedFilterColumns[key] && value != "" {
                        whereConditions = append(whereConditions, fmt.Sprintf("%s = $%d", key, argIndex))
                        args = append(args, value)
                        argIndex++
                }
        }

        whereClause := ""
        if len(whereConditions) > 0 {
                whereClause = "WHERE " + strings.Join(whereConditions, " AND ")
        }

        query := fmt.Sprintf(`
                SELECT id, name, category, status, value, created_at, updated_at
                FROM records
                %s
                ORDER BY %s %s, id %s
                LIMIT $%d
        `, whereClause, sortBy, sortDir, sortDir, argIndex)
        args = append(args, limit+1)

        rows, err := r.pool.Query(ctx, query, args...)
        if err != nil {
                return nil, "", fmt.Errorf("query failed: %w", err)
        }
        defer rows.Close()

        var records []models.Record
        for rows.Next() {
                var record models.Record
                err := rows.Scan(
                        &record.ID,
                        &record.Name,
                        &record.Category,
                        &record.Status,
                        &record.Value,
                        &record.CreatedAt,
                        &record.UpdatedAt,
                )
                if err != nil {
                        return nil, "", fmt.Errorf("scan failed: %w", err)
                }
                records = append(records, record)
        }

        if err := rows.Err(); err != nil {
                return nil, "", fmt.Errorf("rows error: %w", err)
        }

        var nextCursor string
        if len(records) > limit {
                records = records[:limit]
                lastRecord := records[len(records)-1]
                var sortValue interface{}
                switch sortBy {
                case "created_at":
                        sortValue = lastRecord.CreatedAt
                case "updated_at":
                        sortValue = lastRecord.UpdatedAt
                case "name":
                        sortValue = lastRecord.Name
                case "category":
                        sortValue = lastRecord.Category
                default:
                        sortValue = lastRecord.ID
                }
                nextCursor = pagination.EncodeCursor(lastRecord.ID, sortBy, sortValue)
        }

        return records, nextCursor, nil
}

func (r *Repository) GetRecordByID(ctx context.Context, id int64) (*models.Record, error) {
        query := `
                SELECT id, name, category, status, value, created_at, updated_at
                FROM records
                WHERE id = $1
        `

        var record models.Record
        err := r.pool.QueryRow(ctx, query, id).Scan(
                &record.ID,
                &record.Name,
                &record.Category,
                &record.Status,
                &record.Value,
                &record.CreatedAt,
                &record.UpdatedAt,
        )
        if err != nil {
                if err == pgx.ErrNoRows {
                        return nil, nil
                }
                return nil, fmt.Errorf("query failed: %w", err)
        }

        return &record, nil
}

func (r *Repository) SearchRecords(ctx context.Context, searchTerm string, params models.QueryParams) ([]models.Record, string, error) {
        limit := params.Limit
        if limit <= 0 || limit > 50 {
                limit = 20
        }

        var whereConditions []string
        var args []interface{}
        argIndex := 1

        whereConditions = append(whereConditions, fmt.Sprintf("name ILIKE $%d", argIndex))
        args = append(args, "%"+searchTerm+"%")
        argIndex++

        cursor, err := pagination.DecodeCursor(params.Cursor)
        if err != nil {
                return nil, "", fmt.Errorf("invalid cursor: %w", err)
        }

        if cursor != nil {
                whereConditions = append(whereConditions, fmt.Sprintf("id > $%d", argIndex))
                args = append(args, cursor.ID)
                argIndex++
        }

        whereClause := "WHERE " + strings.Join(whereConditions, " AND ")

        query := fmt.Sprintf(`
                SELECT id, name, category, status, value, created_at, updated_at
                FROM records
                %s
                ORDER BY id ASC
                LIMIT $%d
        `, whereClause, argIndex)
        args = append(args, limit+1)

        rows, err := r.pool.Query(ctx, query, args...)
        if err != nil {
                return nil, "", fmt.Errorf("query failed: %w", err)
        }
        defer rows.Close()

        var records []models.Record
        for rows.Next() {
                var record models.Record
                err := rows.Scan(
                        &record.ID,
                        &record.Name,
                        &record.Category,
                        &record.Status,
                        &record.Value,
                        &record.CreatedAt,
                        &record.UpdatedAt,
                )
                if err != nil {
                        return nil, "", fmt.Errorf("scan failed: %w", err)
                }
                records = append(records, record)
        }

        if err := rows.Err(); err != nil {
                return nil, "", fmt.Errorf("rows error: %w", err)
        }

        var nextCursor string
        if len(records) > limit {
                records = records[:limit]
                lastRecord := records[len(records)-1]
                nextCursor = pagination.EncodeCursor(lastRecord.ID, "id", lastRecord.ID)
        }

        return records, nextCursor, nil
}

func (r *Repository) GetStats(ctx context.Context) (map[string]interface{}, error) {
        query := `
                SELECT 
                        COUNT(*) as total_count,
                        COUNT(DISTINCT category) as category_count,
                        COALESCE(SUM(value), 0) as total_value,
                        COALESCE(AVG(value), 0) as avg_value
                FROM records
        `

        var totalCount, categoryCount int64
        var totalValue, avgValue float64

        err := r.pool.QueryRow(ctx, query).Scan(&totalCount, &categoryCount, &totalValue, &avgValue)
        if err != nil {
                return nil, fmt.Errorf("query failed: %w", err)
        }

        return map[string]interface{}{
                "total_count":    totalCount,
                "category_count": categoryCount,
                "total_value":    totalValue,
                "avg_value":      avgValue,
        }, nil
}

func (r *Repository) EnsureSchema(ctx context.Context) error {
        query := `
                CREATE TABLE IF NOT EXISTS records (
                        id BIGSERIAL PRIMARY KEY,
                        name VARCHAR(255) NOT NULL,
                        category VARCHAR(100) NOT NULL,
                        status VARCHAR(50) NOT NULL DEFAULT 'active',
                        value DECIMAL(15,2) NOT NULL DEFAULT 0,
                        created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
                        updated_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
                );

                CREATE INDEX IF NOT EXISTS idx_records_category ON records(category);
                CREATE INDEX IF NOT EXISTS idx_records_status ON records(status);
                CREATE INDEX IF NOT EXISTS idx_records_created_at ON records(created_at);
                CREATE INDEX IF NOT EXISTS idx_records_name ON records(name);
                CREATE INDEX IF NOT EXISTS idx_records_category_created_at ON records(category, created_at);
                CREATE INDEX IF NOT EXISTS idx_records_status_id ON records(status, id);
        `

        _, err := r.pool.Exec(ctx, query)
        if err != nil {
                return fmt.Errorf("failed to ensure schema: %w", err)
        }

        var count int
        err = r.pool.QueryRow(ctx, "SELECT COUNT(*) FROM records").Scan(&count)
        if err != nil {
                return fmt.Errorf("failed to count records: %w", err)
        }

        if count == 0 {
                categories := []string{"electronics", "clothing", "food", "services", "software"}
                statuses := []string{"active", "pending", "completed", "cancelled"}

                for i := 0; i < 100; i++ {
                        name := fmt.Sprintf("Record %d", i+1)
                        category := categories[i%len(categories)]
                        status := statuses[i%len(statuses)]
                        value := float64((i+1)*10) + 0.99

                        _, err := r.pool.Exec(ctx, `
                                INSERT INTO records (name, category, status, value, created_at, updated_at)
                                VALUES ($1, $2, $3, $4, $5, $5)
                        `, name, category, status, value, time.Now().Add(-time.Duration(100-i)*time.Hour))
                        if err != nil {
                                return fmt.Errorf("failed to insert sample data: %w", err)
                        }
                }
        }

        return nil
}

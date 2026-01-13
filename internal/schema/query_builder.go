package schema

import (
        "encoding/base64"
        "encoding/json"
        "fmt"
        "strconv"
        "strings"
        "time"
)

type QueryBuilder struct {
        registry *SchemaRegistry
}

func NewQueryBuilder(registry *SchemaRegistry) *QueryBuilder {
        return &QueryBuilder{registry: registry}
}

type CursorValue struct {
        Column string      `json:"c"`
        Value  interface{} `json:"v"`
        Type   string      `json:"t"`
}

type DynamicCursor struct {
        Values []CursorValue `json:"values"`
}

func (qb *QueryBuilder) EncodeCursor(tableName string, row map[string]interface{}, sortColumn string) string {
        table := qb.registry.GetTable(tableName)
        if table == nil {
                return ""
        }

        var values []CursorValue

        if sortColumn != "" {
                isPK := false
                for _, pk := range table.PrimaryKey {
                        if sortColumn == pk {
                                isPK = true
                                break
                        }
                }
                if !isPK {
                        colType := qb.registry.GetColumnType(tableName, sortColumn)
                        values = append(values, CursorValue{
                                Column: sortColumn,
                                Value:  formatValueForCursor(row[sortColumn], colType),
                                Type:   colType,
                        })
                }
        }

        for _, pk := range table.PrimaryKey {
                colType := qb.registry.GetColumnType(tableName, pk)
                values = append(values, CursorValue{
                        Column: pk,
                        Value:  formatValueForCursor(row[pk], colType),
                        Type:   colType,
                })
        }

        cursor := DynamicCursor{Values: values}
        jsonData, err := json.Marshal(cursor)
        if err != nil {
                return ""
        }

        return base64.URLEncoding.EncodeToString(jsonData)
}

func (qb *QueryBuilder) DecodeCursor(cursorStr string) (*DynamicCursor, error) {
        if cursorStr == "" {
                return nil, nil
        }

        decoded, err := base64.URLEncoding.DecodeString(cursorStr)
        if err != nil {
                return nil, fmt.Errorf("invalid cursor encoding")
        }

        var cursor DynamicCursor
        if err := json.Unmarshal(decoded, &cursor); err != nil {
                return nil, fmt.Errorf("invalid cursor format")
        }

        return &cursor, nil
}

func formatValueForCursor(val interface{}, dataType string) interface{} {
        if val == nil {
                return nil
        }

        switch v := val.(type) {
        case time.Time:
                return v.Format(time.RFC3339Nano)
        default:
                return v
        }
}

func parseValueFromCursor(val interface{}, dataType string) (interface{}, error) {
        if val == nil {
                return nil, nil
        }

        switch {
        case strings.Contains(dataType, "timestamp"), strings.Contains(dataType, "date"):
                if strVal, ok := val.(string); ok {
                        return time.Parse(time.RFC3339Nano, strVal)
                }
        case strings.Contains(dataType, "int"), dataType == "bigint", dataType == "smallint":
                switch v := val.(type) {
                case float64:
                        return int64(v), nil
                case string:
                        return strconv.ParseInt(v, 10, 64)
                case int64:
                        return v, nil
                }
        case strings.Contains(dataType, "numeric"), strings.Contains(dataType, "decimal"), strings.Contains(dataType, "real"), strings.Contains(dataType, "double"):
                switch v := val.(type) {
                case float64:
                        return v, nil
                case string:
                        return strconv.ParseFloat(v, 64)
                }
        }

        return val, nil
}

type QueryParams struct {
        TableName string
        Cursor    string
        Limit     int
        SortBy    string
        SortDir   string
        Filters   map[string]string
}

type BuiltQuery struct {
        SQL        string
        Args       []interface{}
        Columns    []string
        SortColumn string
}

func (qb *QueryBuilder) BuildSelectQuery(params QueryParams) (*BuiltQuery, error) {
        table := qb.registry.GetTable(params.TableName)
        if table == nil {
                return nil, fmt.Errorf("table not found: %s", params.TableName)
        }

        if len(table.PrimaryKey) == 0 {
                return nil, fmt.Errorf("table %s has no primary key", params.TableName)
        }

        var columns []string
        for _, col := range table.Columns {
                columns = append(columns, col.Name)
        }

        sortBy := table.PrimaryKey[0]
        if params.SortBy != "" && qb.registry.IsColumnSortable(params.TableName, params.SortBy) {
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

        cursor, err := qb.DecodeCursor(params.Cursor)
        if err != nil {
                return nil, err
        }

        if cursor != nil && len(cursor.Values) > 0 {
                cursorCondition, cursorArgs, nextArgIndex, err := qb.buildCursorCondition(cursor, sortDir, argIndex)
                if err != nil {
                        return nil, err
                }
                if cursorCondition != "" {
                        whereConditions = append(whereConditions, cursorCondition)
                        args = append(args, cursorArgs...)
                        argIndex = nextArgIndex
                }
        }

        for col, val := range params.Filters {
                if val != "" && qb.registry.IsColumnSortable(params.TableName, col) {
                        whereConditions = append(whereConditions, fmt.Sprintf("%s = $%d", col, argIndex))
                        args = append(args, val)
                        argIndex++
                }
        }

        whereClause := ""
        if len(whereConditions) > 0 {
                whereClause = "WHERE " + strings.Join(whereConditions, " AND ")
        }

        var orderParts []string
        sortByIsPK := false
        for _, pk := range table.PrimaryKey {
                if sortBy == pk {
                        sortByIsPK = true
                        break
                }
        }

        if !sortByIsPK {
                orderParts = append(orderParts, fmt.Sprintf("%s %s", sortBy, sortDir))
        }

        for _, pk := range table.PrimaryKey {
                orderParts = append(orderParts, fmt.Sprintf("%s %s", pk, sortDir))
        }

        query := fmt.Sprintf(`
                SELECT %s
                FROM %s
                %s
                ORDER BY %s
                LIMIT $%d
        `, strings.Join(columns, ", "), params.TableName, whereClause, strings.Join(orderParts, ", "), argIndex)
        args = append(args, limit+1)

        return &BuiltQuery{
                SQL:        query,
                Args:       args,
                Columns:    columns,
                SortColumn: sortBy,
        }, nil
}

func (qb *QueryBuilder) buildCursorCondition(cursor *DynamicCursor, sortDir string, startArgIndex int) (string, []interface{}, int, error) {
        if len(cursor.Values) == 0 {
                return "", nil, startArgIndex, nil
        }

        op := ">"
        if sortDir == "DESC" {
                op = "<"
        }

        var parsedValues []interface{}
        for _, cv := range cursor.Values {
                pv, err := parseValueFromCursor(cv.Value, cv.Type)
                if err != nil {
                        return "", nil, startArgIndex, err
                }
                parsedValues = append(parsedValues, pv)
        }

        if len(cursor.Values) == 1 {
                condition := fmt.Sprintf("%s %s $%d", cursor.Values[0].Column, op, startArgIndex)
                return condition, []interface{}{parsedValues[0]}, startArgIndex + 1, nil
        }

        var conditions []string
        var args []interface{}
        argIndex := startArgIndex
        n := len(cursor.Values)

        for i := 0; i < n; i++ {
                var parts []string
                var branchArgs []interface{}

                for j := 0; j < i; j++ {
                        parts = append(parts, fmt.Sprintf("%s = $%d", cursor.Values[j].Column, argIndex))
                        branchArgs = append(branchArgs, parsedValues[j])
                        argIndex++
                }

                parts = append(parts, fmt.Sprintf("%s %s $%d", cursor.Values[i].Column, op, argIndex))
                branchArgs = append(branchArgs, parsedValues[i])
                argIndex++

                args = append(args, branchArgs...)
                conditions = append(conditions, "("+strings.Join(parts, " AND ")+")")
        }

        return "(" + strings.Join(conditions, " OR ") + ")", args, argIndex, nil
}

func (qb *QueryBuilder) BuildGetByPKQuery(tableName string, pkValue interface{}) (*BuiltQuery, error) {
        table := qb.registry.GetTable(tableName)
        if table == nil {
                return nil, fmt.Errorf("table not found: %s", tableName)
        }

        if len(table.PrimaryKey) == 0 {
                return nil, fmt.Errorf("table %s has no primary key", tableName)
        }

        var columns []string
        for _, col := range table.Columns {
                columns = append(columns, col.Name)
        }

        query := fmt.Sprintf(`
                SELECT %s
                FROM %s
                WHERE %s = $1
        `, strings.Join(columns, ", "), tableName, table.PrimaryKey[0])

        return &BuiltQuery{
                SQL:     query,
                Args:    []interface{}{pkValue},
                Columns: columns,
        }, nil
}

func (qb *QueryBuilder) BuildSearchQuery(params QueryParams, searchColumn, searchTerm string) (*BuiltQuery, error) {
        table := qb.registry.GetTable(params.TableName)
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

        var whereConditions []string
        var args []interface{}
        argIndex := 1

        whereConditions = append(whereConditions, fmt.Sprintf("%s ILIKE $%d", searchColumn, argIndex))
        args = append(args, "%"+searchTerm+"%")
        argIndex++

        cursor, err := qb.DecodeCursor(params.Cursor)
        if err != nil {
                return nil, err
        }

        if cursor != nil && len(cursor.Values) > 0 {
                cursorCondition, cursorArgs, nextArgIndex, err := qb.buildCursorCondition(cursor, "ASC", argIndex)
                if err != nil {
                        return nil, err
                }
                if cursorCondition != "" {
                        whereConditions = append(whereConditions, cursorCondition)
                        args = append(args, cursorArgs...)
                        argIndex = nextArgIndex
                }
        }

        whereClause := "WHERE " + strings.Join(whereConditions, " AND ")

        var orderParts []string
        for _, pk := range table.PrimaryKey {
                orderParts = append(orderParts, fmt.Sprintf("%s ASC", pk))
        }

        query := fmt.Sprintf(`
                SELECT %s
                FROM %s
                %s
                ORDER BY %s
                LIMIT $%d
        `, strings.Join(columns, ", "), params.TableName, whereClause, strings.Join(orderParts, ", "), argIndex)
        args = append(args, limit+1)

        return &BuiltQuery{
                SQL:        query,
                Args:       args,
                Columns:    columns,
                SortColumn: table.PrimaryKey[0],
        }, nil
}

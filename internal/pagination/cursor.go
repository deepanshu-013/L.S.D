package pagination

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"strconv"
	"time"
)

type CursorData struct {
	ID        int64     `json:"id"`
	CreatedAt time.Time `json:"created_at,omitempty"`
	SortField string    `json:"sort_field"`
	SortValue string    `json:"sort_value"`
}

var ErrInvalidCursor = errors.New("invalid cursor")

func EncodeCursor(id int64, sortField string, sortValue interface{}) string {
	var sortValueStr string
	switch v := sortValue.(type) {
	case time.Time:
		sortValueStr = v.Format(time.RFC3339Nano)
	case int64:
		sortValueStr = strconv.FormatInt(v, 10)
	case float64:
		sortValueStr = strconv.FormatFloat(v, 'f', -1, 64)
	case string:
		sortValueStr = v
	default:
		sortValueStr = ""
	}

	data := CursorData{
		ID:        id,
		SortField: sortField,
		SortValue: sortValueStr,
	}

	jsonData, err := json.Marshal(data)
	if err != nil {
		return ""
	}

	return base64.URLEncoding.EncodeToString(jsonData)
}

func DecodeCursor(cursor string) (*CursorData, error) {
	if cursor == "" {
		return nil, nil
	}

	decoded, err := base64.URLEncoding.DecodeString(cursor)
	if err != nil {
		return nil, ErrInvalidCursor
	}

	var data CursorData
	if err := json.Unmarshal(decoded, &data); err != nil {
		return nil, ErrInvalidCursor
	}

	return &data, nil
}

func ParseSortValueAsTime(sortValue string) (time.Time, error) {
	return time.Parse(time.RFC3339Nano, sortValue)
}

func ParseSortValueAsInt64(sortValue string) (int64, error) {
	return strconv.ParseInt(sortValue, 10, 64)
}

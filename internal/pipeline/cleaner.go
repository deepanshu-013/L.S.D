package pipeline

import (
	"fmt"
	"math"
	"regexp"
	"strconv"
	"strings"
	"unicode"
)

// ============================================================================
// COLUMN NAME CLEANING
// ============================================================================

// CleanColumnNames standardizes column names for PostgreSQL compatibility
// - Converts to lowercase
// - Replaces special characters with underscores
// - Handles duplicates with numeric suffixes
// - Ensures valid SQL identifiers
func CleanColumnNames(columns []string) []string {
	cleaned := make([]string, 0, len(columns))
	seen := make(map[string]int, len(columns))
	specialCharsRe := regexp.MustCompile(`[^a-zA-Z0-9_]+`)
	multiUnderscoreRe := regexp.MustCompile(`_+`)

	for idx, col := range columns {
		// Normalize whitespace and remove special characters
		colStr := strings.TrimSpace(col)
		colClean := specialCharsRe.ReplaceAllString(colStr, "_")
		colClean = multiUnderscoreRe.ReplaceAllString(colClean, "_")
		colClean = strings.ToLower(strings.Trim(colClean, "_"))

		// Handle empty or invalid names
		if colClean == "" || isNumericString(colClean) {
			colClean = fmt.Sprintf("column_%d", idx)
		}

		// Ensure doesn't start with number (PostgreSQL requirement)
		if len(colClean) > 0 && unicode.IsDigit(rune(colClean[0])) {
			colClean = "col_" + colClean
		}

		// Handle duplicates
		if count, exists := seen[colClean]; exists {
			seen[colClean] = count + 1
			colClean = fmt.Sprintf("%s_%d", colClean, count+1)
		} else {
			seen[colClean] = 0
		}

		cleaned = append(cleaned, colClean)
	}

	return cleaned
}

// ============================================================================
// DATA CLEANING
// ============================================================================

// CleanData removes completely empty rows from the dataset
func CleanData(rows [][]string) [][]string {
	if len(rows) == 0 {
		return rows
	}

	cleaned := make([][]string, 0, len(rows))
	for _, row := range rows {
		if !isEmptyRow(row) {
			cleaned = append(cleaned, row)
		}
	}
	return cleaned
}

// ============================================================================
// EMPTY COLUMN DETECTION
// ============================================================================

// IdentifyEmptyColumns finds columns exceeding the empty threshold
// threshold: 0.99 means 99% of values are empty/null
func IdentifyEmptyColumns(rows [][]string, header []string, threshold float64) map[int]bool {
	emptyColumns := make(map[int]bool)
	totalRows := len(rows)

	if totalRows == 0 {
		return emptyColumns
	}

	for colIdx := range header {
		emptyCount := 0

		for _, row := range rows {
			if colIdx >= len(row) || strings.TrimSpace(row[colIdx]) == "" {
				emptyCount++
			}
		}

		emptyRatio := float64(emptyCount) / float64(totalRows)
		if emptyRatio > threshold {
			emptyColumns[colIdx] = true
		}
	}

	return emptyColumns
}

// RemoveEmptyColumns filters out columns identified as empty
func RemoveEmptyColumns(rows [][]string, header []string, emptyColumns map[int]bool) ([][]string, []string) {
	if len(emptyColumns) == 0 {
		return rows, header
	}

	// Filter header
	newHeader := make([]string, 0, len(header))
	for i, col := range header {
		if !emptyColumns[i] {
			newHeader = append(newHeader, col)
		}
	}

	// Filter rows
	newRows := make([][]string, 0, len(rows))
	for _, row := range rows {
		newRow := make([]string, 0, len(newHeader))
		for i, val := range row {
			if !emptyColumns[i] {
				newRow = append(newRow, val)
			}
		}
		newRows = append(newRows, newRow)
	}

	return newRows, newHeader
}

// RemoveColumnsFromSchema removes columns from schema definition
func RemoveColumnsFromSchema(header []string, types map[string]string, emptyColumns map[int]bool) ([]string, map[string]string) {
	newHeader := make([]string, 0, len(header))
	newTypes := make(map[string]string, len(header))

	for i, col := range header {
		if !emptyColumns[i] {
			newHeader = append(newHeader, col)
			newTypes[col] = types[col]
		}
	}

	return newHeader, newTypes
}

// ============================================================================
// TYPE INFERENCE (OPTIMIZED)
// ============================================================================

// InferColumnTypes performs intelligent type detection with stratified sampling
// Uses PostgreSQL-optimized types and TEXT for all string columns (safest approach)
func InferColumnTypes(rows [][]string, header []string) map[string]string {
	types := make(map[string]string, len(header))

	if len(rows) == 0 {
		// Default to TEXT for empty datasets
		for _, col := range header {
			types[col] = "TEXT"
		}
		return types
	}

	// Calculate optimal sample size
	totalRows := len(rows)
	sampleIndices := getStratifiedSample(totalRows, calculateSampleSize(totalRows))

	// Analyze each column
	for colIdx, colName := range header {
		stats := analyzeColumn(rows, colIdx, sampleIndices)
		types[colName] = determineColumnType(stats)
	}

	return types
}

// ColumnStats holds statistical information about a column
type ColumnStats struct {
	IntCount        int
	FloatCount      int
	BoolCount       int
	DateCount       int
	TotalNonEmpty   int
	MaxLength       int
	MinLength       int
	HasLeadingZeros bool
}

// analyzeColumn performs statistical analysis on a single column
func analyzeColumn(rows [][]string, colIdx int, sampleIndices []int) ColumnStats {
	stats := ColumnStats{
		MinLength: math.MaxInt32,
	}

	for _, rowIdx := range sampleIndices {
		if rowIdx >= len(rows) || colIdx >= len(rows[rowIdx]) {
			continue
		}

		val := strings.TrimSpace(rows[rowIdx][colIdx])
		if val == "" {
			continue
		}

		stats.TotalNonEmpty++
		valLen := len(val)

		if valLen > stats.MaxLength {
			stats.MaxLength = valLen
		}
		if valLen < stats.MinLength {
			stats.MinLength = valLen
		}

		// Check for leading zeros (indicates should be string, not number)
		if len(val) > 1 && val[0] == '0' && val[1] != '.' {
			stats.HasLeadingZeros = true
		}

		// Skip type detection if leading zeros detected
		if stats.HasLeadingZeros {
			continue
		}

		// Try integer parsing
		if _, err := strconv.ParseInt(val, 10, 64); err == nil {
			stats.IntCount++
			continue
		}

		// Try float parsing (with cleanup)
		if isNumericWithCleanup(val) {
			stats.FloatCount++
			continue
		}

		// Try boolean detection
		if isBooleanValue(val) {
			stats.BoolCount++
			continue
		}

		// Try date detection (optional - can be expensive)
		// if isDateValue(val) {
		//     stats.DateCount++
		// }
	}

	return stats
}

// determineColumnType applies decision logic to assign PostgreSQL type
func determineColumnType(stats ColumnStats) string {
	// Empty column
	if stats.TotalNonEmpty == 0 {
		return "TEXT"
	}

	// Leading zeros indicate string (zip codes, phone numbers, etc.)
	if stats.HasLeadingZeros {
		return "TEXT"
	}

	// Calculate threshold (50% of non-empty values)
	threshold := float64(stats.TotalNonEmpty) * 0.5

	// Integer column - use BIGINT for safety
	if float64(stats.IntCount) > threshold {
		return "BIGINT"
	}

	// Numeric column - supports decimals, currency, percentages
	if float64(stats.FloatCount) > threshold {
		return "NUMERIC"
	}

	// Boolean column - only if very consistent
	if float64(stats.BoolCount) > threshold && stats.TotalNonEmpty <= 100 {
		return "BOOLEAN"
	}

	// ⭐ STRING COLUMNS - ALWAYS USE TEXT (SAFEST APPROACH)
	// This eliminates ALL truncation issues and matches Python pipeline behavior
	return "TEXT"

	// Alternative: Use VARCHAR with safety buffer (NOT RECOMMENDED)
	// if stats.MaxLength > 255 {
	//     return "TEXT"
	// } else {
	//     safeLength := min(stats.MaxLength * 3, 500)
	//     return fmt.Sprintf("VARCHAR(%d)", safeLength)
	// }
}

// ============================================================================
// SAMPLING STRATEGIES
// ============================================================================

// calculateSampleSize determines optimal sample size based on dataset size
func calculateSampleSize(totalRows int) int {
	switch {
	case totalRows <= 1000:
		return totalRows // Use all rows for small datasets
	case totalRows <= 10000:
		return 2000 // 20% sample
	case totalRows <= 50000:
		return 5000 // 10% sample
	case totalRows <= 100000:
		return 8000 // 8% sample
	case totalRows <= 500000:
		return 10000 // 2% sample
	case totalRows <= 1000000:
		return 15000 // 1.5% sample
	default:
		return 20000 // Cap at 20k for massive files (0.1% for 20M+ rows)
	}
}

// getStratifiedSample creates evenly distributed sample indices
// Ensures we sample from beginning, middle, and end of dataset
func getStratifiedSample(totalRows, sampleSize int) []int {
	if totalRows <= sampleSize {
		indices := make([]int, totalRows)
		for i := range indices {
			indices[i] = i
		}
		return indices
	}

	indices := make([]int, sampleSize)
	step := float64(totalRows) / float64(sampleSize)

	for i := 0; i < sampleSize; i++ {
		indices[i] = int(math.Floor(float64(i) * step))
	}

	return indices
}

// ============================================================================
// NUMERIC DETECTION HELPERS
// ============================================================================

// isNumericWithCleanup attempts to parse value as float after cleanup
func isNumericWithCleanup(s string) bool {
	cleaned := cleanNumericString(s)
	_, err := strconv.ParseFloat(cleaned, 64)
	return err == nil
}

// cleanNumericString removes common currency/formatting characters
func cleanNumericString(s string) string {
	s = strings.ReplaceAll(s, ",", "") // Thousands separator
	s = strings.ReplaceAll(s, " ", "") // Spaces
	s = strings.TrimPrefix(s, "$")     // US dollar
	s = strings.TrimPrefix(s, "€")     // Euro
	s = strings.TrimPrefix(s, "£")     // British pound
	s = strings.TrimPrefix(s, "¥")     // Yen/Yuan
	s = strings.TrimPrefix(s, "₹")     // Indian rupee
	s = strings.TrimSuffix(s, "%")     // Percentage
	return strings.TrimSpace(s)
}

// ============================================================================
// BOOLEAN DETECTION
// ============================================================================

// isBooleanValue checks if string represents a boolean
func isBooleanValue(s string) bool {
	lower := strings.ToLower(strings.TrimSpace(s))
	switch lower {
	case "true", "false", "t", "f",
		"yes", "no", "y", "n",
		"1", "0",
		"on", "off",
		"enabled", "disabled":
		return true
	}
	return false
}

// ============================================================================
// JSON FLATTENING
// ============================================================================

// FlattenJSON recursively flattens nested JSON structures
// Arrays are converted to JSON strings to avoid type issues
func FlattenJSON(data interface{}, prefix string, result map[string]interface{}) {
	switch v := data.(type) {
	case map[string]interface{}:
		for key, value := range v {
			newKey := key
			if prefix != "" {
				newKey = prefix + "_" + key
			}
			FlattenJSON(value, newKey, result)
		}

	case []interface{}:
		// Convert arrays to JSON strings to avoid unhashable types
		result[prefix] = fmt.Sprintf("%v", v)

	default:
		result[prefix] = v
	}
}

// ============================================================================
// UTILITY FUNCTIONS
// ============================================================================

// isEmptyRow checks if all values in a row are empty
func isEmptyRow(row []string) bool {
	for _, val := range row {
		if strings.TrimSpace(val) != "" {
			return false
		}
	}
	return true
}

// isNumericString checks if string is purely numeric
func isNumericString(s string) bool {
	if s == "" {
		return false
	}

	// Compile regex once (could be moved to package level for performance)
	matched, _ := regexp.MatchString(`^-?\d+\.?\d*$`, s)
	return matched
}

// min returns the smaller of two integers
func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

// max returns the larger of two integers
func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}

// ============================================================================
// OPTIONAL: DATE DETECTION (COMMENTED OUT FOR PERFORMANCE)
// ============================================================================

// Uncomment if you need date type detection
// Note: This adds ~10-15% overhead to type inference

/*
import "time"

var commonDateFormats = []string{
	"2006-01-02",           // ISO 8601
	"01/02/2006",           // US format
	"02/01/2006",           // European format
	"2006/01/02",           // ISO variant
	"Jan 02, 2006",         // Long format
	"02-Jan-2006",          // Alternative
	time.RFC3339,           // RFC3339
	time.RFC822,            // RFC822
}

func isDateValue(s string) bool {
	s = strings.TrimSpace(s)
	if s == "" {
		return false
	}

	for _, format := range commonDateFormats {
		if _, err := time.Parse(format, s); err == nil {
			return true
		}
	}
	return false
}

// If enabling date detection, add to determineColumnType():
// if float64(stats.DateCount) > threshold {
//     return "DATE"
// }
*/

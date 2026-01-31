package pipeline

import (
	"bufio"
	"os"
	"strconv"
	"strings"

	"github.com/saintfish/chardet"
	"golang.org/x/text/encoding/charmap"
	"golang.org/x/text/encoding/unicode"
	"golang.org/x/text/transform"
)

// DetectEncoding detects file encoding (UTF-8, Latin-1, Windows-1252, etc.)
func DetectEncoding(filePath string) (string, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return "", err
	}
	defer file.Close()

	// Read first 100KB for analysis
	buffer := make([]byte, 100000)
	n, _ := file.Read(buffer)

	detector := chardet.NewTextDetector()
	result, err := detector.DetectBest(buffer[:n])

	if err != nil || result.Confidence < 70 {
		return "UTF-8", nil // Fallback
	}

	return result.Charset, nil
}

// GetDecoder returns the appropriate transform.Transformer for encoding
func GetDecoder(encoding string) transform.Transformer {
	switch encoding {
	case "ISO-8859-1", "Latin-1":
		return charmap.ISO8859_1.NewDecoder()
	case "Windows-1252":
		return charmap.Windows1252.NewDecoder()
	case "UTF-16LE":
		return unicode.UTF16(unicode.LittleEndian, unicode.IgnoreBOM).NewDecoder()
	case "UTF-16BE":
		return unicode.UTF16(unicode.BigEndian, unicode.IgnoreBOM).NewDecoder()
	default:
		return unicode.UTF8.NewDecoder()
	}
}

// DetectDelimiter analyzes CSV file to determine the best delimiter
func DetectDelimiter(filePath string, encoding string) (rune, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return ',', err
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)

	// Read first 10 lines
	var lines []string
	for i := 0; i < 10 && scanner.Scan(); i++ {
		lines = append(lines, scanner.Text())
	}

	if len(lines) == 0 {
		return ',', nil
	}

	// Test delimiters: comma, semicolon, tab, pipe, space
	delimiters := []rune{',', ';', '\t', '|', ' '}
	maxCols := 0
	bestDelim := ','

	for _, delim := range delimiters {
		consistent := true
		firstCount := -1
		avgCount := 0

		for _, line := range lines {
			count := strings.Count(line, string(delim))
			avgCount += count

			if firstCount == -1 {
				firstCount = count
			} else if count != firstCount {
				consistent = false
			}
		}

		if len(lines) > 0 {
			avgCount = avgCount / len(lines)
		}

		// Prefer consistent delimiters with more columns
		if consistent && firstCount > maxCols {
			maxCols = firstCount
			bestDelim = delim
		} else if !consistent && avgCount > maxCols {
			maxCols = avgCount
			bestDelim = delim
		}
	}

	return bestDelim, nil
}

// DetectHeader determines if the first row is a header or data
func DetectHeader(filePath string, encoding string, delimiter rune) (bool, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return false, err
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)

	// Read first 2 lines
	var lines []string
	for i := 0; i < 2 && scanner.Scan(); i++ {
		lines = append(lines, scanner.Text())
	}

	if len(lines) < 2 {
		return false, nil
	}

	row1 := strings.Split(lines[0], string(delimiter))
	row2 := strings.Split(lines[1], string(delimiter))

	if len(row1) == 0 || len(row2) == 0 {
		return false, nil
	}

	// Check if first row is all strings (non-numeric)
	allStrings := true
	for _, val := range row1 {
		val = strings.TrimSpace(val)
		if val == "" {
			continue
		}

		// If it's purely numeric, probably not a header
		if _, err := strconv.ParseFloat(val, 64); err == nil {
			allStrings = false
			break
		}

		// If too long (>50 chars), probably data
		if len(val) > 50 {
			allStrings = false
			break
		}

		// If contains special characters typical in data
		if strings.Contains(val, "@") || strings.Contains(val, "://") {
			allStrings = false
			break
		}
	}

	// Check if second row has numbers
	hasNumbers := false
	for _, val := range row2 {
		val = strings.TrimSpace(val)
		if val == "" {
			continue
		}

		if _, err := strconv.ParseFloat(val, 64); err == nil {
			hasNumbers = true
			break
		}
	}

	// Header detection: first row is strings AND second row has numbers
	return allStrings && (hasNumbers || len(row1[0]) < 30), nil
}

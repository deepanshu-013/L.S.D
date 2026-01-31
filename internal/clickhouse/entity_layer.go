package clickhouse

import (
	"context"
	"crypto/md5"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"log"
	"strings"
	"sync"
	"time"
)

// Entity represents a searchable entity (person, company, product)
type Entity struct {
	EntityID       uint64    `json:"entity_id"`
	EntityType     string    `json:"entity_type"`
	SourceTable    string    `json:"source_table"`
	RowID          string    `json:"row_id"`
	EntityName     string    `json:"entity_name"`
	SearchableText string    `json:"searchable_text"`
	Tokens         []string  `json:"tokens"`
	CreatedAt      time.Time `json:"created_at"`
	UpdatedAt      time.Time `json:"updated_at"`
}

type EntityRepository struct {
	conn    ClickHouseConnection
	mu      sync.RWMutex
	enabled bool
}

func NewEntityRepository(conn ClickHouseConnection) *EntityRepository {
	return &EntityRepository{
		conn:    conn,
		enabled: conn != nil && conn.IsAvailable(),
	}
}

func (r *EntityRepository) IsEnabled() bool {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.enabled
}

func (r *EntityRepository) Initialize(ctx context.Context) error {
	log.Println("🏗️  Initializing Three-Layer Search Architecture...")

	if err := r.CreateEntityLayer(ctx); err != nil {
		return fmt.Errorf("failed to create entity layer: %w", err)
	}

	if err := r.CreateTokenLayer(ctx); err != nil {
		return fmt.Errorf("failed to create token layer: %w", err)
	}

	r.mu.Lock()
	r.enabled = true
	r.mu.Unlock()

	log.Println("✅ Three-Layer Search Architecture Ready!")
	return nil
}

func (r *EntityRepository) CreateEntityLayer(ctx context.Context) error {
	log.Println("Creating Entity Layer (Layer 2)...")

	entityTableSQL := `
	CREATE TABLE IF NOT EXISTS search_entities (
		entity_id UInt64,
		entity_type LowCardinality(String),
		source_table LowCardinality(String),
		row_id String,
		entity_name String,
		searchable_text String,
		created_at DateTime DEFAULT now(),
		updated_at DateTime DEFAULT now(),
		
		INDEX idx_entity_name entity_name TYPE tokenbf_v1(32768, 3, 0) GRANULARITY 4,
		INDEX idx_searchable searchable_text TYPE tokenbf_v1(32768, 3, 0) GRANULARITY 4
	) ENGINE = ReplacingMergeTree(updated_at)
	PARTITION BY toYYYYMM(created_at)
	ORDER BY (entity_type, entity_id)
	SETTINGS
		index_granularity = 8192,
		compress_marks = 1,
		compress_primary_key = 1
	`

	if err := r.conn.Exec(ctx, entityTableSQL); err != nil {
		return fmt.Errorf("failed to create entity table: %w", err)
	}

	log.Println("✅ Entity Layer created")
	return nil
}

func (r *EntityRepository) CreateTokenLayer(ctx context.Context) error {
	log.Println("Creating Token Layer (Layer 3)...")

	tokenTableSQL := `
    CREATE TABLE IF NOT EXISTS search_tokens (
        token_hash UInt64,
        token LowCardinality(String),
        entity_ids Array(UInt64),
        frequency UInt32 DEFAULT 0,
        last_seen DateTime DEFAULT now(),
        
        INDEX idx_token token TYPE tokenbf_v1(65536, 3, 0) GRANULARITY 1
    ) ENGINE = ReplacingMergeTree(last_seen)
    ORDER BY (token_hash, token)
    SETTINGS
        index_granularity = 4096,
        index_granularity_bytes = 10485760
    `

	if err := r.conn.Exec(ctx, tokenTableSQL); err != nil {
		return fmt.Errorf("failed to create token table: %w", err)
	}

	log.Println("✅ Token Layer created")
	return nil
}

func (r *EntityRepository) GenerateEntityID(tableName, rowID string) uint64 {
	hash := md5.Sum([]byte(tableName + ":" + rowID))
	return binary.BigEndian.Uint64(hash[:8])
}

func (r *EntityRepository) ExtractEntity(tableName string, rowID string, data map[string]interface{}) *Entity {
	entityID := r.GenerateEntityID(tableName, rowID)
	entityType := r.detectEntityType(data)
	entityName := r.extractEntityName(data, entityType)
	searchableText := r.buildSearchableText(data)
	tokens := r.extractTokens(searchableText)

	return &Entity{
		EntityID:       entityID,
		EntityType:     entityType,
		SourceTable:    tableName,
		RowID:          rowID,
		EntityName:     entityName,
		SearchableText: searchableText,
		Tokens:         tokens,
		CreatedAt:      time.Now(),
		UpdatedAt:      time.Now(),
	}
}

func (r *EntityRepository) detectEntityType(data map[string]interface{}) string {
	if _, hasFirstName := data["first_name"]; hasFirstName {
		return "person"
	}
	if _, hasLastName := data["last_name"]; hasLastName {
		return "person"
	}
	if _, hasCompany := data["company_name"]; hasCompany {
		return "company"
	}
	if _, hasProduct := data["product_name"]; hasProduct {
		return "product"
	}
	return "generic"
}

func (r *EntityRepository) extractEntityName(data map[string]interface{}, entityType string) string {
	switch entityType {
	case "person":
		firstName := r.getStringValue(data, "first_name")
		lastName := r.getStringValue(data, "last_name")
		return strings.TrimSpace(firstName + " " + lastName)
	case "company":
		return r.getStringValue(data, "company_name")
	case "product":
		return r.getStringValue(data, "product_name")
	default:
		if name := r.getStringValue(data, "name"); name != "" {
			return name
		}
		return "Unknown"
	}
}

func (r *EntityRepository) getStringValue(data map[string]interface{}, key string) string {
	if val, ok := data[key]; ok && val != nil {
		return fmt.Sprintf("%v", val)
	}
	return ""
}

func (r *EntityRepository) buildSearchableText(data map[string]interface{}) string {
	var parts []string
	priorityFields := []string{"first_name", "last_name", "company_name", "email1", "mobile1", "city", "state"}

	for _, field := range priorityFields {
		if val := r.getStringValue(data, field); val != "" {
			parts = append(parts, val)
		}
	}

	for key, value := range data {
		if value != nil {
			valStr := fmt.Sprintf("%v", value)
			if valStr != "" && valStr != "0" && valStr != "false" && !contains(priorityFields, key) {
				parts = append(parts, valStr)
			}
		}
	}

	return strings.ToLower(strings.Join(parts, " "))
}

func contains(slice []string, item string) bool {
	for _, s := range slice {
		if s == item {
			return true
		}
	}
	return false
}

func (r *EntityRepository) extractTokens(text string) []string {
	tokenMap := make(map[string]bool)
	words := strings.Fields(strings.ToLower(text))

	for _, word := range words {
		word = strings.Trim(word, ".,;:!?\"'()[]{}@#$%^&*+-=<>")
		if len(word) >= 2 && len(word) <= 50 && !isNumericOnly(word) {
			tokenMap[word] = true
		}
	}

	var tokens []string
	for token := range tokenMap {
		tokens = append(tokens, token)
	}

	return tokens
}

func isNumericOnly(s string) bool {
	for _, r := range s {
		if r < '0' || r > '9' {
			return false
		}
	}
	return true
}

func (r *EntityRepository) BulkIndexEntities(ctx context.Context, entities []*Entity) error {
	if !r.IsEnabled() || len(entities) == 0 {
		return nil
	}

	batch, err := r.conn.PrepareBatch(ctx, `
		INSERT INTO search_entities (
			entity_id, entity_type, source_table, row_id,
			entity_name, searchable_text, updated_at
		)
	`)
	if err != nil {
		return fmt.Errorf("failed to prepare batch: %w", err)
	}

	for _, entity := range entities {
		if err := batch.Append(
			entity.EntityID,
			entity.EntityType,
			entity.SourceTable,
			entity.RowID,
			entity.EntityName,
			entity.SearchableText,
			entity.UpdatedAt,
		); err != nil {
			log.Printf("Failed to append entity %d: %v", entity.EntityID, err)
		}
	}

	if err := batch.Send(); err != nil {
		return fmt.Errorf("failed to send batch: %w", err)
	}

	tokenEntityMap := make(map[string][]uint64)
	for _, entity := range entities {
		for _, token := range entity.Tokens {
			tokenEntityMap[token] = append(tokenEntityMap[token], entity.EntityID)
		}
	}

	tokenBatch, err := r.conn.PrepareBatch(ctx, `
		INSERT INTO search_tokens (token_hash, token, entity_ids, frequency, last_seen)
	`)
	if err != nil {
		return fmt.Errorf("failed to prepare token batch: %w", err)
	}

	for token, entityIDs := range tokenEntityMap {
		tokenHash := r.hashToken(token)
		if err := tokenBatch.Append(
			tokenHash,
			token,
			entityIDs,
			uint32(len(entityIDs)),
			time.Now(),
		); err != nil {
			log.Printf("Failed to append token '%s': %v", token, err)
		}
	}

	if err := tokenBatch.Send(); err != nil {
		return fmt.Errorf("failed to send token batch: %w", err)
	}

	log.Printf("[EntityRepo] Indexed %d entities with %d unique tokens", len(entities), len(tokenEntityMap))
	return nil
}

func (r *EntityRepository) hashToken(token string) uint64 {
	hash := md5.Sum([]byte(token))
	return binary.BigEndian.Uint64(hash[:8])
}

func (r *EntityRepository) SearchThreeLayer(ctx context.Context, searchTerm string, limit int) ([]map[string]interface{}, int, error) {
	if !r.IsEnabled() {
		return nil, 0, fmt.Errorf("entity search not enabled")
	}

	tokens := r.extractTokens(searchTerm)
	if len(tokens) == 0 {
		return nil, 0, fmt.Errorf("no valid tokens")
	}

	log.Printf("[Layer 3] Searching %d tokens: %v", len(tokens), tokens)

	var entityIDSets [][]uint64
	for _, token := range tokens {
		tokenHash := r.hashToken(token)
		query := `
		SELECT entity_ids
		FROM search_tokens FINAL
		WHERE token_hash = ? AND token = ?
		LIMIT 1
		`

		var entityIDs []uint64
		row := r.conn.QueryRow(ctx, query, tokenHash, token)
		if err := row.Scan(&entityIDs); err == nil && len(entityIDs) > 0 {
			entityIDSets = append(entityIDSets, entityIDs)
			log.Printf("[Layer 3] Token '%s' → %d entities", token, len(entityIDs))
		} else {
			log.Printf("[Layer 3] Token '%s' → 0 entities", token)
		}
	}

	if len(entityIDSets) == 0 {
		return []map[string]interface{}{}, 0, nil
	}

	commonEntityIDs := r.intersectEntityIDs(entityIDSets)
	log.Printf("[Layer 2] Found %d matching entities", len(commonEntityIDs))

	if len(commonEntityIDs) == 0 {
		return []map[string]interface{}{}, 0, nil
	}

	if len(commonEntityIDs) > limit*2 {
		commonEntityIDs = commonEntityIDs[:limit*2]
	}

	var results []map[string]interface{}
	for _, entityID := range commonEntityIDs {
		if len(results) >= limit {
			break
		}

		entityQuery := `
		SELECT source_table, row_id, entity_name, entity_type
		FROM search_entities FINAL
		WHERE entity_id = ?
		LIMIT 1
		`

		var sourceTable, rowID, entityName, entityType string
		row := r.conn.QueryRow(ctx, entityQuery, entityID)
		if err := row.Scan(&sourceTable, &rowID, &entityName, &entityType); err != nil {
			continue
		}

		tableQuery := fmt.Sprintf(`
		SELECT original_data
		FROM search_%s
		WHERE id = ? AND is_deleted = 0
		LIMIT 1
		`, sourceTable)

		var originalData string
		tableRow := r.conn.QueryRow(ctx, tableQuery, rowID)
		if err := tableRow.Scan(&originalData); err != nil {
			continue
		}

		var record map[string]interface{}
		if err := json.Unmarshal([]byte(originalData), &record); err != nil {
			continue
		}

		record["_entity_id"] = entityID
		record["_entity_name"] = entityName
		record["_entity_type"] = entityType
		record["_source_table"] = sourceTable

		results = append(results, record)
	}

	log.Printf("[Layer 1] Fetched %d rows", len(results))
	return results, len(commonEntityIDs), nil
}

func (r *EntityRepository) intersectEntityIDs(sets [][]uint64) []uint64 {
	if len(sets) == 0 {
		return []uint64{}
	}
	if len(sets) == 1 {
		return sets[0]
	}

	common := make(map[uint64]int)
	for _, id := range sets[0] {
		common[id] = 1
	}

	for i := 1; i < len(sets); i++ {
		for _, id := range sets[i] {
			if _, exists := common[id]; exists {
				common[id]++
			}
		}
	}

	var result []uint64
	for id, count := range common {
		if count == len(sets) {
			result = append(result, id)
		}
	}

	return result
}

func (r *EntityRepository) GetStats(ctx context.Context) (map[string]interface{}, error) {
	if !r.IsEnabled() {
		return map[string]interface{}{"enabled": false}, nil
	}

	var entityCount, tokenCount uint64

	row := r.conn.QueryRow(ctx, "SELECT count() FROM search_entities")
	if err := row.Scan(&entityCount); err != nil {
		entityCount = 0
	}

	row = r.conn.QueryRow(ctx, "SELECT count() FROM search_tokens")
	if err := row.Scan(&tokenCount); err != nil {
		tokenCount = 0
	}

	return map[string]interface{}{
		"enabled":       true,
		"entity_count":  entityCount,
		"token_count":   tokenCount,
		"search_method": "three_layer_entity",
	}, nil
}

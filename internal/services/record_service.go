package services

import (
	"context"
	"time"

	"highperf-api/internal/cache"
	"highperf-api/internal/database"
	"highperf-api/internal/models"
)

type RecordService struct {
	repo    *database.Repository
	cache   *cache.RedisCache
	timeout time.Duration
}

func NewRecordService(repo *database.Repository, cache *cache.RedisCache, timeout time.Duration) *RecordService {
	return &RecordService{
		repo:    repo,
		cache:   cache,
		timeout: timeout,
	}
}

func (s *RecordService) GetRecords(ctx context.Context, params models.QueryParams) (*models.PaginatedResponse, error) {
	ctx, cancel := context.WithTimeout(ctx, s.timeout)
	defer cancel()

	// ✅ FIXED: Added sortBy and sortDir parameters (empty strings as defaults)
	cacheKey := s.cache.GenerateCacheKey("records", params.Filters, params.Cursor, params.Limit, "", "")

	var cachedResponse models.PaginatedResponse
	if hit, _ := s.cache.Get(ctx, cacheKey, &cachedResponse); hit {
		return &cachedResponse, nil
	}

	records, nextCursor, err := s.repo.GetRecords(ctx, params)
	if err != nil {
		return nil, err
	}

	response := &models.PaginatedResponse{
		Data:       records,
		NextCursor: nextCursor,
		HasMore:    nextCursor != "",
		Count:      len(records),
	}

	s.cache.Set(ctx, cacheKey, response)

	return response, nil
}

func (s *RecordService) GetRecordByID(ctx context.Context, id int64) (*models.Record, error) {
	ctx, cancel := context.WithTimeout(ctx, s.timeout)
	defer cancel()

	return s.repo.GetRecordByID(ctx, id)
}

func (s *RecordService) SearchRecords(ctx context.Context, searchTerm string, params models.QueryParams) (*models.PaginatedResponse, error) {
	ctx, cancel := context.WithTimeout(ctx, s.timeout)
	defer cancel()

	filters := map[string]string{"search": searchTerm}
	// ✅ FIXED: Added sortBy and sortDir parameters (empty strings as defaults)
	cacheKey := s.cache.GenerateCacheKey("search", filters, params.Cursor, params.Limit, "", "")

	var cachedResponse models.PaginatedResponse
	if hit, _ := s.cache.Get(ctx, cacheKey, &cachedResponse); hit {
		return &cachedResponse, nil
	}

	records, nextCursor, err := s.repo.SearchRecords(ctx, searchTerm, params)
	if err != nil {
		return nil, err
	}

	response := &models.PaginatedResponse{
		Data:       records,
		NextCursor: nextCursor,
		HasMore:    nextCursor != "",
		Count:      len(records),
	}

	s.cache.Set(ctx, cacheKey, response)

	return response, nil
}

func (s *RecordService) GetStats(ctx context.Context) (map[string]interface{}, error) {
	ctx, cancel := context.WithTimeout(ctx, s.timeout)
	defer cancel()

	// ✅ FIXED: Added sortBy and sortDir parameters (empty strings as defaults)
	cacheKey := s.cache.GenerateCacheKey("stats", nil, "", 0, "", "")

	var cachedStats map[string]interface{}
	if hit, _ := s.cache.Get(ctx, cacheKey, &cachedStats); hit {
		return cachedStats, nil
	}

	stats, err := s.repo.GetStats(ctx)
	if err != nil {
		return nil, err
	}

	s.cache.Set(ctx, cacheKey, stats)

	return stats, nil
}

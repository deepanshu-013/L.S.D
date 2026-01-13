package handlers

import (
	"encoding/json"
	"net/http"
	"strconv"

	"highperf-api/internal/models"
	"highperf-api/internal/services"
)

type APIHandler struct {
	service     *services.RecordService
	maxPageSize int
	defaultSize int
}

func NewAPIHandler(service *services.RecordService, maxPageSize, defaultSize int) *APIHandler {
	return &APIHandler{
		service:     service,
		maxPageSize: maxPageSize,
		defaultSize: defaultSize,
	}
}

func (h *APIHandler) GetRecords(w http.ResponseWriter, r *http.Request) {
	params := h.parseQueryParams(r)

	response, err := h.service.GetRecords(r.Context(), params)
	if err != nil {
		h.writeError(w, http.StatusInternalServerError, "Failed to fetch records")
		return
	}

	h.writeJSON(w, http.StatusOK, response)
}

func (h *APIHandler) GetRecordByID(w http.ResponseWriter, r *http.Request) {
	idStr := r.PathValue("id")
	if idStr == "" {
		h.writeError(w, http.StatusBadRequest, "ID is required")
		return
	}

	id, err := strconv.ParseInt(idStr, 10, 64)
	if err != nil {
		h.writeError(w, http.StatusBadRequest, "Invalid ID format")
		return
	}

	record, err := h.service.GetRecordByID(r.Context(), id)
	if err != nil {
		h.writeError(w, http.StatusInternalServerError, "Failed to fetch record")
		return
	}

	if record == nil {
		h.writeError(w, http.StatusNotFound, "Record not found")
		return
	}

	h.writeJSON(w, http.StatusOK, record)
}

func (h *APIHandler) SearchRecords(w http.ResponseWriter, r *http.Request) {
	searchTerm := r.URL.Query().Get("q")
	if searchTerm == "" {
		h.writeError(w, http.StatusBadRequest, "Search term is required")
		return
	}

	if len(searchTerm) < 2 || len(searchTerm) > 100 {
		h.writeError(w, http.StatusBadRequest, "Search term must be 2-100 characters")
		return
	}

	params := h.parseQueryParams(r)

	response, err := h.service.SearchRecords(r.Context(), searchTerm, params)
	if err != nil {
		h.writeError(w, http.StatusInternalServerError, "Failed to search records")
		return
	}

	h.writeJSON(w, http.StatusOK, response)
}

func (h *APIHandler) GetStats(w http.ResponseWriter, r *http.Request) {
	stats, err := h.service.GetStats(r.Context())
	if err != nil {
		h.writeError(w, http.StatusInternalServerError, "Failed to fetch stats")
		return
	}

	h.writeJSON(w, http.StatusOK, stats)
}

func (h *APIHandler) HealthCheck(w http.ResponseWriter, r *http.Request) {
	h.writeJSON(w, http.StatusOK, map[string]string{
		"status":  "healthy",
		"service": "highperf-api",
	})
}

func (h *APIHandler) parseQueryParams(r *http.Request) models.QueryParams {
	params := models.QueryParams{
		Cursor:  r.URL.Query().Get("cursor"),
		SortBy:  r.URL.Query().Get("sort_by"),
		SortDir: r.URL.Query().Get("sort_dir"),
		Filters: make(map[string]string),
	}

	if limitStr := r.URL.Query().Get("limit"); limitStr != "" {
		if limit, err := strconv.Atoi(limitStr); err == nil {
			params.Limit = limit
		}
	}

	if params.Limit <= 0 {
		params.Limit = h.defaultSize
	}
	if params.Limit > h.maxPageSize {
		params.Limit = h.maxPageSize
	}

	allowedFilters := []string{"category", "status"}
	for _, key := range allowedFilters {
		if value := r.URL.Query().Get(key); value != "" {
			params.Filters[key] = value
		}
	}

	return params
}

func (h *APIHandler) writeJSON(w http.ResponseWriter, status int, data interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(data)
}

func (h *APIHandler) writeError(w http.ResponseWriter, status int, message string) {
	h.writeJSON(w, status, map[string]string{"error": message})
}

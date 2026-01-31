package handlers

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"time"

	"highperf-api/internal/pipeline"

	"github.com/google/uuid"
)

type PipelineHandler struct {
	processor *pipeline.PipelineProcessor
}

func NewPipelineHandler(processor *pipeline.PipelineProcessor) *PipelineHandler {
	return &PipelineHandler{
		processor: processor,
	}
}

type StartJobRequest struct {
	FolderPath string `json:"folder_path"`
	Recursive  bool   `json:"recursive"`
}

// POST /api/pipeline/start
func (h *PipelineHandler) StartJob(w http.ResponseWriter, r *http.Request) {
	var req StartJobRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		h.writeError(w, http.StatusBadRequest, "Invalid request body")
		return
	}

	if req.FolderPath == "" {
		h.writeError(w, http.StatusBadRequest, "folder_path is required")
		return
	}

	jobID := uuid.New().String()

	if err := h.processor.StartJob(r.Context(), jobID, req.FolderPath, req.Recursive); err != nil {
		h.writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	h.writeJSON(w, http.StatusAccepted, map[string]interface{}{
		"job_id":  jobID,
		"message": "Job started successfully",
	})
}

// GET /api/pipeline/jobs/{job_id}
func (h *PipelineHandler) GetJobStatus(w http.ResponseWriter, r *http.Request) {
	jobID := r.PathValue("job_id")
	if jobID == "" {
		h.writeError(w, http.StatusBadRequest, "job_id is required")
		return
	}

	progress, err := h.processor.GetJobProgress(jobID)
	if err != nil {
		h.writeError(w, http.StatusNotFound, err.Error())
		return
	}

	h.writeJSON(w, http.StatusOK, progress)
}

// GET /api/pipeline/jobs
func (h *PipelineHandler) ListJobs(w http.ResponseWriter, r *http.Request) {
	jobs := h.processor.ListJobs()
	h.writeJSON(w, http.StatusOK, map[string]interface{}{
		"jobs":  jobs,
		"count": len(jobs),
	})
}

// GET /api/pipeline/jobs/{job_id}/stream (SSE for live progress)
func (h *PipelineHandler) StreamJobProgress(w http.ResponseWriter, r *http.Request) {
	jobID := r.PathValue("job_id")

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")

	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "Streaming not supported", http.StatusInternalServerError)
		return
	}

	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-r.Context().Done():
			return
		case <-ticker.C:
			progress, err := h.processor.GetJobProgress(jobID)
			if err != nil {
				return
			}

			data, _ := json.Marshal(progress)
			fmt.Fprintf(w, "data: %s\n\n", data)
			flusher.Flush()

			if progress.Status == "completed" || progress.Status == "failed" {
				return
			}
		}
	}
}

// GET /api/pipeline/jobs/{job_id}/logs (NEW - View logs)
func (h *PipelineHandler) GetJobLogs(w http.ResponseWriter, r *http.Request) {
	jobID := r.PathValue("job_id")
	if jobID == "" {
		h.writeError(w, http.StatusBadRequest, "job_id is required")
		return
	}

	progress, err := h.processor.GetJobProgress(jobID)
	if err != nil {
		h.writeError(w, http.StatusNotFound, err.Error())
		return
	}

	if progress.LogPath == "" {
		h.writeError(w, http.StatusNotFound, "Log file not found")
		return
	}

	// Read log file
	logFile, err := os.Open(progress.LogPath)
	if err != nil {
		h.writeError(w, http.StatusInternalServerError, "Failed to open log file")
		return
	}
	defer logFile.Close()

	// Set headers
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	w.WriteHeader(http.StatusOK)

	// Stream log content
	io.Copy(w, logFile)
}

func (h *PipelineHandler) writeJSON(w http.ResponseWriter, status int, data interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(data)
}

func (h *PipelineHandler) writeError(w http.ResponseWriter, status int, message string) {
	h.writeJSON(w, status, map[string]string{"error": message})
}

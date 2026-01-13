package handlers

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"strings"

	"highperf-api/internal/models"
	"highperf-api/internal/services"
)

type BotHandler struct {
	service *services.RecordService
}

func NewBotHandler(service *services.RecordService) *BotHandler {
	return &BotHandler{service: service}
}

func (h *BotHandler) TelegramWebhook(w http.ResponseWriter, r *http.Request) {
	var update models.TelegramUpdate
	if err := json.NewDecoder(r.Body).Decode(&update); err != nil {
		http.Error(w, `{"error":"invalid request"}`, http.StatusBadRequest)
		return
	}

	if update.Message == nil || update.Message.Text == "" {
		w.WriteHeader(http.StatusOK)
		return
	}

	response := h.processCommand(r, update.Message.Text)

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"method":  "sendMessage",
		"chat_id": update.Message.Chat.ID,
		"text":    response,
	})
}

func (h *BotHandler) WhatsAppWebhook(w http.ResponseWriter, r *http.Request) {
	if r.Method == "GET" {
		mode := r.URL.Query().Get("hub.mode")
		token := r.URL.Query().Get("hub.verify_token")
		challenge := r.URL.Query().Get("hub.challenge")

		if mode == "subscribe" && token != "" {
			w.WriteHeader(http.StatusOK)
			w.Write([]byte(challenge))
			return
		}
		http.Error(w, "Forbidden", http.StatusForbidden)
		return
	}

	var message models.WhatsAppMessage
	if err := json.NewDecoder(r.Body).Decode(&message); err != nil {
		http.Error(w, `{"error":"invalid request"}`, http.StatusBadRequest)
		return
	}

	for _, entry := range message.Entry {
		for _, change := range entry.Changes {
			for _, msg := range change.Value.Messages {
				if msg.Text != nil {
					response := h.processCommand(r, msg.Text.Body)
					json.NewEncoder(w).Encode(models.BotResponse{
						Success: true,
						Message: response,
					})
					return
				}
			}
		}
	}

	w.WriteHeader(http.StatusOK)
}

func (h *BotHandler) processCommand(r *http.Request, text string) string {
	text = strings.TrimSpace(text)
	parts := strings.Fields(text)

	if len(parts) == 0 {
		return h.helpMessage()
	}

	command := strings.ToLower(parts[0])

	switch command {
	case "/start", "/help":
		return h.helpMessage()

	case "/list":
		return h.handleList(r, parts[1:])

	case "/search":
		return h.handleSearch(r, parts[1:])

	case "/get":
		return h.handleGet(r, parts[1:])

	case "/stats":
		return h.handleStats(r)

	default:
		return "Unknown command. Type /help for available commands."
	}
}

func (h *BotHandler) helpMessage() string {
	return `Dynamic API Bot - Available commands:
/list [table] - List records from a table
/search <table> <term> - Search records
/get <table> <id> - Get record details by ID
/stats - Get API statistics
/help - Show this help message

This bot works with any table in your database!`
}

func (h *BotHandler) handleList(r *http.Request, args []string) string {
	if h.service == nil {
		return "Bot service not configured. Use the API directly at /api/tables"
	}

	params := models.QueryParams{
		Limit:   10,
		Filters: make(map[string]string),
	}

	if len(args) > 0 {
		params.Filters["category"] = args[0]
	}

	response, err := h.service.GetRecords(r.Context(), params)
	if err != nil {
		return "Error fetching records. Please try again."
	}

	if len(response.Data) == 0 {
		return "No records found."
	}

	var result strings.Builder
	result.WriteString(fmt.Sprintf("Found %d records:\n\n", response.Count))

	for i, record := range response.Data {
		if i >= 5 {
			result.WriteString(fmt.Sprintf("\n... and %d more", response.Count-5))
			break
		}
		result.WriteString(fmt.Sprintf("%d. %s (%s) - $%.2f\n", record.ID, record.Name, record.Category, record.Value))
	}

	if response.HasMore {
		result.WriteString("\nMore records available.")
	}

	return result.String()
}

func (h *BotHandler) handleSearch(r *http.Request, args []string) string {
	if len(args) == 0 {
		return "Usage: /search <term>"
	}

	if h.service == nil {
		return "Bot service not configured. Use the API directly at /api/tables/{table}/search?q=term"
	}

	searchTerm := strings.Join(args, " ")
	if len(searchTerm) < 2 {
		return "Search term must be at least 2 characters."
	}

	params := models.QueryParams{Limit: 10}

	response, err := h.service.SearchRecords(r.Context(), searchTerm, params)
	if err != nil {
		return "Error searching records. Please try again."
	}

	if len(response.Data) == 0 {
		return fmt.Sprintf("No records found matching '%s'.", searchTerm)
	}

	var result strings.Builder
	result.WriteString(fmt.Sprintf("Search results for '%s':\n\n", searchTerm))

	for i, record := range response.Data {
		if i >= 5 {
			result.WriteString(fmt.Sprintf("\n... and %d more", response.Count-5))
			break
		}
		result.WriteString(fmt.Sprintf("%d. %s (%s)\n", record.ID, record.Name, record.Category))
	}

	return result.String()
}

func (h *BotHandler) handleGet(r *http.Request, args []string) string {
	if len(args) == 0 {
		return "Usage: /get <id>"
	}

	if h.service == nil {
		return "Bot service not configured. Use the API directly at /api/tables/{table}/records/{id}"
	}

	id, err := strconv.ParseInt(args[0], 10, 64)
	if err != nil {
		return "Invalid ID. Please provide a numeric ID."
	}

	record, err := h.service.GetRecordByID(r.Context(), id)
	if err != nil {
		return "Error fetching record. Please try again."
	}

	if record == nil {
		return fmt.Sprintf("Record with ID %d not found.", id)
	}

	return fmt.Sprintf(`Record Details:
ID: %d
Name: %s
Category: %s
Status: %s
Value: $%.2f
Created: %s`,
		record.ID, record.Name, record.Category, record.Status, record.Value,
		record.CreatedAt.Format("2006-01-02 15:04"))
}

func (h *BotHandler) handleStats(r *http.Request) string {
	if h.service == nil {
		return "Bot service not configured. Use the API directly at /api/tables to list all tables."
	}

	stats, err := h.service.GetStats(r.Context())
	if err != nil {
		return "Error fetching statistics. Please try again."
	}

	return fmt.Sprintf(`Database Statistics:
Total Records: %.0f
Categories: %.0f
Total Value: $%.2f
Average Value: $%.2f`,
		stats["total_count"],
		stats["category_count"],
		stats["total_value"],
		stats["avg_value"])
}

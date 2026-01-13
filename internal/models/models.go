package models

import (
	"time"
)

type Record struct {
	ID        int64     `json:"id"`
	Name      string    `json:"name"`
	Category  string    `json:"category"`
	Status    string    `json:"status"`
	Value     float64   `json:"value"`
	CreatedAt time.Time `json:"created_at"`
	UpdatedAt time.Time `json:"updated_at"`
}

type PaginatedResponse struct {
	Data       []Record `json:"data"`
	NextCursor string   `json:"next_cursor,omitempty"`
	HasMore    bool     `json:"has_more"`
	Count      int      `json:"count"`
}

type QueryParams struct {
	Cursor   string
	Limit    int
	SortBy   string
	SortDir  string
	Filters  map[string]string
}

type BotCommand struct {
	Command string            `json:"command"`
	Args    map[string]string `json:"args,omitempty"`
}

type BotResponse struct {
	Success bool        `json:"success"`
	Message string      `json:"message,omitempty"`
	Data    interface{} `json:"data,omitempty"`
}

type TelegramUpdate struct {
	UpdateID int64           `json:"update_id"`
	Message  *TelegramMessage `json:"message,omitempty"`
}

type TelegramMessage struct {
	MessageID int64         `json:"message_id"`
	From      *TelegramUser `json:"from,omitempty"`
	Chat      *TelegramChat `json:"chat"`
	Text      string        `json:"text,omitempty"`
}

type TelegramUser struct {
	ID        int64  `json:"id"`
	FirstName string `json:"first_name"`
	Username  string `json:"username,omitempty"`
}

type TelegramChat struct {
	ID   int64  `json:"id"`
	Type string `json:"type"`
}

type WhatsAppMessage struct {
	Object string          `json:"object"`
	Entry  []WhatsAppEntry `json:"entry,omitempty"`
}

type WhatsAppEntry struct {
	ID      string            `json:"id"`
	Changes []WhatsAppChange  `json:"changes,omitempty"`
}

type WhatsAppChange struct {
	Value WhatsAppValue `json:"value"`
	Field string        `json:"field"`
}

type WhatsAppValue struct {
	MessagingProduct string              `json:"messaging_product"`
	Messages         []WhatsAppMsg       `json:"messages,omitempty"`
}

type WhatsAppMsg struct {
	From string          `json:"from"`
	ID   string          `json:"id"`
	Type string          `json:"type"`
	Text *WhatsAppText   `json:"text,omitempty"`
}

type WhatsAppText struct {
	Body string `json:"body"`
}

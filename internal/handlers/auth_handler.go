package handlers

import (
	"encoding/json"
	"highperf-api/internal/auth"
	"net/http"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

type AuthHandler struct {
	db          *pgxpool.Pool
	authService *auth.AuthService
}

func NewAuthHandler(db *pgxpool.Pool, service *auth.AuthService) *AuthHandler {
	return &AuthHandler{
		db:          db,
		authService: service,
	}
}

// ShowLoginPage serves the login.html file
func (h *AuthHandler) ShowLoginPage(w http.ResponseWriter, r *http.Request) {
	http.ServeFile(w, r, "./login.html")
}

// Login verifies credentials and sets the API Key as an HttpOnly cookie
func (h *AuthHandler) Login(w http.ResponseWriter, r *http.Request) {
	var creds struct {
		Username string `json:"username"`
		Password string `json:"password"`
	}

	if err := json.NewDecoder(r.Body).Decode(&creds); err != nil {
		http.Error(w, "Invalid request", http.StatusBadRequest)
		return
	}

	// 1. Fetch User from DB
	var id int
	var role, hash, apiKey string
	query := `SELECT id, role, password_hash, api_key FROM users WHERE username = $1`
	err := h.db.QueryRow(r.Context(), query, creds.Username).Scan(&id, &role, &hash, &apiKey)
	if err != nil {
		http.Error(w, `{"error": "Invalid credentials"}`, http.StatusUnauthorized)
		return
	}

	// 2. Verify Password
	if !h.authService.CheckPasswordHash(creds.Password, hash) {
		http.Error(w, `{"error": "Invalid credentials"}`, http.StatusUnauthorized)
		return
	}

	// 3. Set API Key in HttpOnly Cookie
	// The browser will send this automatically on every request
	http.SetCookie(w, &http.Cookie{
		Name:     "api_key", // Cookie name
		Value:    apiKey,    // The actual API Key
		Path:     "/",
		Expires:  time.Now().Add(7 * 24 * time.Hour), // 1 week
		HttpOnly: true,                               // JavaScript cannot read it
		Secure:   false,                              // Set to true for HTTPS in production
		SameSite: http.SameSiteLaxMode,
	})

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{
		"status": "logged_in",
		"role":   role,
	})
}

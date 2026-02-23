package handlers

import (
	"encoding/json"
	"highperf-api/internal/auth"
	"net/http"

	"github.com/jackc/pgx/v5/pgxpool"
)

type AuthHandler struct {
	db          *pgxpool.Pool
	authService *auth.AuthService
}

// Updated to accept *pgxpool.Pool
func NewAuthHandler(db *pgxpool.Pool, service *auth.AuthService) *AuthHandler {
	return &AuthHandler{
		db:          db,
		authService: service,
	}
}

type LoginRequest struct {
	Username string `json:"username"`
	Password string `json:"password"`
}

type RegisterRequest struct {
	Username string `json:"username"`
	Password string `json:"password"`
	Role     string `json:"role"`
}

func (h *AuthHandler) Login(w http.ResponseWriter, r *http.Request) {
	var req LoginRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Invalid request body", http.StatusBadRequest)
		return
	}

	var id int
	var username, role, hash string

	// Query using pgxpool
	query := `SELECT id, username, role, password_hash FROM users WHERE username = $1`
	err := h.db.QueryRow(r.Context(), query, req.Username).Scan(&id, &username, &role, &hash)
	if err != nil {
		http.Error(w, `{"error": "Invalid credentials"}`, http.StatusUnauthorized)
		return
	}

	if !h.authService.CheckPasswordHash(req.Password, hash) {
		http.Error(w, `{"error": "Invalid credentials"}`, http.StatusUnauthorized)
		return
	}

	token, err := h.authService.GenerateToken(id, username, role)
	if err != nil {
		http.Error(w, "Failed to generate token", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{
		"token": token,
	})
}

func (h *AuthHandler) Register(w http.ResponseWriter, r *http.Request) {
	var req RegisterRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Invalid request body", http.StatusBadRequest)
		return
	}

	if req.Role == "" {
		req.Role = "user"
	}

	hash, err := h.authService.HashPassword(req.Password)
	if err != nil {
		http.Error(w, "Failed to hash password", http.StatusInternalServerError)
		return
	}

	query := `INSERT INTO users (username, password_hash, role) VALUES ($1, $2, $3) RETURNING id`
	var id int
	err = h.db.QueryRow(r.Context(), query, req.Username, hash, req.Role).Scan(&id)
	if err != nil {
		http.Error(w, `{"error": "Username already exists or DB error"}`, http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	json.NewEncoder(w).Encode(map[string]interface{}{
		"id":       id,
		"username": req.Username,
		"role":     req.Role,
	})
}

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

func NewAuthHandler(db *pgxpool.Pool, service *auth.AuthService) *AuthHandler {
	return &AuthHandler{
		db:          db,
		authService: service,
	}
}

func (h *AuthHandler) Login(w http.ResponseWriter, r *http.Request) {
	var username, password string

	// 1. Check if GET request (Browser URL mode)
	if r.Method == http.MethodGet {
		username = r.URL.Query().Get("username")
		password = r.URL.Query().Get("password")
	} else {
		// 2. Fallback to POST (JSON body mode)
		var req struct {
			Username string `json:"username"`
			Password string `json:"password"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, "Invalid request body", http.StatusBadRequest)
			return
		}
		username = req.Username
		password = req.Password
	}

	if username == "" || password == "" {
		http.Error(w, `{"error": "Username and password required"}`, http.StatusBadRequest)
		return
	}

	// 3. Verify User
	var id int
	var role, hash string
	query := `SELECT id, role, password_hash FROM users WHERE username = $1`
	err := h.db.QueryRow(r.Context(), query, username).Scan(&id, &role, &hash)
	if err != nil {
		http.Error(w, `{"error": "User not found"}`, http.StatusUnauthorized)
		return
	}

	if !h.authService.CheckPasswordHash(password, hash) {
		http.Error(w, `{"error": "Invalid credentials"}`, http.StatusUnauthorized)
		return
	}

	// 4. Generate Token
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

// Register remains POST-only for safety, or you can make it GET similarly if needed.
func (h *AuthHandler) Register(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Username string `json:"username"`
		Password string `json:"password"`
		Role     string `json:"role"`
	}
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

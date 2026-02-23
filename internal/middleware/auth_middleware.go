package middleware

import (
	"context"
	"highperf-api/internal/auth"
	"net/http"
	"strings"
)

// Key type for context values
type ContextKey string

const UserKey ContextKey = "user"

type AuthMiddleware struct {
	authService *auth.AuthService
}

func NewAuthMiddleware(service *auth.AuthService) *AuthMiddleware {
	return &AuthMiddleware{authService: service}
}

// RequireAuth is a middleware wrapper that checks for a valid JWT
func (m *AuthMiddleware) RequireAuth(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		authHeader := r.Header.Get("Authorization")
		if authHeader == "" {
			http.Error(w, `{"error": "Authorization header missing"}`, http.StatusUnauthorized)
			return
		}

		// Expected format: "Bearer <token>"
		parts := strings.Split(authHeader, " ")
		if len(parts) != 2 || parts[0] != "Bearer" {
			http.Error(w, `{"error": "Invalid authorization format. Use: Bearer <token>"}`, http.StatusUnauthorized)
			return
		}

		tokenString := parts[1]
		claims, err := m.authService.ValidateToken(tokenString)
		if err != nil {
			http.Error(w, `{"error": "Invalid or expired token"}`, http.StatusUnauthorized)
			return
		}

		// Inject user info into request context
		ctx := context.WithValue(r.Context(), UserKey, claims)
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

package middleware

import (
	"context"
	"highperf-api/internal/auth"
	"net/http"
	"strings"
)

type ContextKey string

const UserKey ContextKey = "user"

type AuthMiddleware struct {
	authService *auth.AuthService
}

func NewAuthMiddleware(service *auth.AuthService) *AuthMiddleware {
	return &AuthMiddleware{authService: service}
}

func (m *AuthMiddleware) RequireAuth(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var tokenString string

		// 1. Check Authorization Header (Bearer Token)
		authHeader := r.Header.Get("Authorization")
		if strings.HasPrefix(authHeader, "Bearer ") {
			tokenString = strings.TrimPrefix(authHeader, "Bearer ")
		}

		// 2. Fallback: Check URL Query Parameter (?token=...)
		if tokenString == "" {
			tokenString = r.URL.Query().Get("token")
		}

		// 3. If still no token, reject
		if tokenString == "" {
			http.Error(w, `{"error": "Authentication required. Use Header or ?token=..."}`, http.StatusUnauthorized)
			return
		}

		// 4. Validate
		claims, err := m.authService.ValidateToken(tokenString)
		if err != nil {
			http.Error(w, `{"error": "Invalid or expired token"}`, http.StatusUnauthorized)
			return
		}

		// Inject user info into context
		ctx := context.WithValue(r.Context(), UserKey, claims)
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

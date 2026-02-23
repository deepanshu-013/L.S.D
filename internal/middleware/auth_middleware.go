package middleware

import (
	"context"
	"net/http"

	"github.com/jackc/pgx/v5"
)

type ContextKey string

const UserKey ContextKey = "user"

// queryDB interface now matches the exact signature of pgxpool.Pool
type queryDB interface {
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

type AuthMiddleware struct {
	db queryDB
}

func NewAuthMiddleware(db queryDB) *AuthMiddleware {
	return &AuthMiddleware{db: db}
}

func (m *AuthMiddleware) RequireAuth(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var apiKey string

		// 1. Check Cookie (Browser Auto-Login)
		cookie, err := r.Cookie("api_key")
		if err == nil {
			apiKey = cookie.Value
		}

		// 2. Check Header (For external API clients / Postman)
		if apiKey == "" {
			apiKey = r.Header.Get("X-API-Key")
		}
		if apiKey == "" {
			authHeader := r.Header.Get("Authorization")
			if len(authHeader) > 7 && authHeader[:7] == "Bearer " {
				apiKey = authHeader[7:]
			}
		}

		// 3. No key found
		if apiKey == "" {
			http.Error(w, `{"error": "Authentication required. Provide X-API-Key header or login."}`, http.StatusUnauthorized)
			return
		}

		// 4. Validate Key against Database
		var username string
		var role string
		query := `SELECT username, role FROM users WHERE api_key = $1`

		// Use pgx.Row directly
		row := m.db.QueryRow(r.Context(), query, apiKey)
		err = row.Scan(&username, &role)

		if err != nil {
			http.Error(w, `{"error": "Invalid API Key"}`, http.StatusUnauthorized)
			return
		}

		// 5. Inject user info into context
		ctx := context.WithValue(r.Context(), UserKey, map[string]string{
			"username": username,
			"role":     role,
		})
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

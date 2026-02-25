package auth

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"golang.org/x/crypto/bcrypt"
)

// Configuration
const (
	AccessTokenExpiry  = 15 * time.Minute
	RefreshTokenExpiry = 7 * 24 * time.Hour
	APIKeyPrefix       = "lsd_live_"
	BcryptCost         = 12
)

// JWTClaims represents the claims in a JWT token
type JWTClaims struct {
	UserID   string   `json:"user_id"`
	Email    string   `json:"email"`
	Username string   `json:"username"`
	Role     string   `json:"role"`
	Scopes   []string `json:"scopes"`
	jwt.RegisteredClaims
}

// TokenType represents the type of JWT token
type TokenType string

const (
	AccessToken  TokenType = "access"
	RefreshToken TokenType = "refresh"
)

// APIKeyScope represents available API key scopes
type APIKeyScope string

const (
	ScopeRead     APIKeyScope = "read"
	ScopeWrite    APIKeyScope = "write"
	ScopeSearch   APIKeyScope = "search"
	ScopePipeline APIKeyScope = "pipeline"
	ScopeAdmin    APIKeyScope = "admin"
)

// AuthService handles authentication operations
type AuthService struct {
	jwtSecret []byte
}

// NewAuthService creates a new AuthService
func NewAuthService(jwtSecret string) *AuthService {
	return &AuthService{
		jwtSecret: []byte(jwtSecret),
	}
}

// HashPassword generates a bcrypt hash
func (s *AuthService) HashPassword(password string) (string, error) {
	bytes, err := bcrypt.GenerateFromPassword([]byte(password), BcryptCost)
	return string(bytes), err
}

// CheckPasswordHash compares password with hash
func (s *AuthService) CheckPasswordHash(password, hash string) bool {
	err := bcrypt.CompareHashAndPassword([]byte(hash), []byte(password))
	return err == nil
}

// GenerateAccessToken creates a JWT access token
func (s *AuthService) GenerateAccessToken(userID, email, username, role string, scopes []string) (string, error) {
	now := time.Now()
	claims := JWTClaims{
		UserID:   userID,
		Email:    email,
		Username: username,
		Role:     role,
		Scopes:   scopes,
		RegisteredClaims: jwt.RegisteredClaims{
			ExpiresAt: jwt.NewNumericDate(now.Add(AccessTokenExpiry)),
			IssuedAt:  jwt.NewNumericDate(now),
			Issuer:    "lsd-api",
			Audience:  []string{"lsd-api-users"},
		},
	}

	token := jwt.NewWithClaims(jwt.SigningMethodHS256, claims)
	return token.SignedString(s.jwtSecret)
}

// GenerateRefreshToken creates a JWT refresh token
func (s *AuthService) GenerateRefreshToken(userID string) (string, error) {
	now := time.Now()
	claims := jwt.RegisteredClaims{
		Subject:   userID,
		ExpiresAt: jwt.NewNumericDate(now.Add(RefreshTokenExpiry)),
		IssuedAt:  jwt.NewNumericDate(now),
		Issuer:    "lsd-api",
		Audience:  []string{"lsd-api-users"},
	}

	token := jwt.NewWithClaims(jwt.SigningMethodHS256, claims)
	return token.SignedString(s.jwtSecret)
}

// VerifyAccessToken validates an access token and returns claims
func (s *AuthService) VerifyAccessToken(tokenString string) (*JWTClaims, error) {
	token, err := jwt.ParseWithClaims(tokenString, &JWTClaims{}, func(token *jwt.Token) (interface{}, error) {
		if _, ok := token.Method.(*jwt.SigningMethodHMAC); !ok {
			return nil, fmt.Errorf("unexpected signing method: %v", token.Header["alg"])
		}
		return s.jwtSecret, nil
	})

	if err != nil {
		return nil, err
	}

	if claims, ok := token.Claims.(*JWTClaims); ok && token.Valid {
		return claims, nil
	}

	return nil, fmt.Errorf("invalid token")
}

// VerifyRefreshToken validates a refresh token and returns the user ID
func (s *AuthService) VerifyRefreshToken(tokenString string) (string, error) {
	token, err := jwt.Parse(tokenString, func(token *jwt.Token) (interface{}, error) {
		if _, ok := token.Method.(*jwt.SigningMethodHMAC); !ok {
			return nil, fmt.Errorf("unexpected signing method: %v", token.Header["alg"])
		}
		return s.jwtSecret, nil
	})

	if err != nil {
		return "", err
	}

	if claims, ok := token.Claims.(jwt.MapClaims); ok && token.Valid {
		if userID, ok := claims["sub"].(string); ok {
			return userID, nil
		}
	}

	return "", fmt.Errorf("invalid token")
}

// GenerateAPIKey creates a new API key
func (s *AuthService) GenerateAPIKey() (key, keyHash, keyPrefix string) {
	// Generate random bytes
	randomBytes := make([]byte, 24)
	rand.Read(randomBytes)
	randomPart := hex.EncodeToString(randomBytes)

	key = APIKeyPrefix + randomPart

	// Hash for storage
	hash := sha256.Sum256([]byte(key))
	keyHash = hex.EncodeToString(hash[:])

	// Prefix for identification
	keyPrefix = key[:16]

	return key, keyHash, keyPrefix
}

// HashAPIKey hashes an API key for verification
func (s *AuthService) HashAPIKey(key string) string {
	hash := sha256.Sum256([]byte(key))
	return hex.EncodeToString(hash[:])
}

// HasScope checks if a scope list contains a required scope
func HasScope(scopes []string, required string) bool {
	// Admin has all permissions
	for _, s := range scopes {
		if s == string(ScopeAdmin) {
			return true
		}
		if s == required {
			return true
		}
	}
	return false
}

// ValidateScopes validates that all provided scopes are valid
func ValidateScopes(scopes []string) (valid []string, invalid []string) {
	validScopes := map[string]bool{
		string(ScopeRead):     true,
		string(ScopeWrite):    true,
		string(ScopeSearch):   true,
		string(ScopePipeline): true,
		string(ScopeAdmin):    true,
	}

	for _, scope := range scopes {
		if validScopes[scope] {
			valid = append(valid, scope)
		} else {
			invalid = append(invalid, scope)
		}
	}

	return valid, invalid
}

// ValidatePasswordStrength checks if password meets requirements
func ValidatePasswordStrength(password string) (valid bool, errors []string) {
	if len(password) < 8 {
		errors = append(errors, "Password must be at least 8 characters")
	}
	hasUpper := false
	hasLower := false
	hasNumber := false
	hasSpecial := false

	for _, char := range password {
		switch {
		case 'A' <= char && char <= 'Z':
			hasUpper = true
		case 'a' <= char && char <= 'z':
			hasLower = true
		case '0' <= char && char <= '9':
			hasNumber = true
		default:
			hasSpecial = true
		}
	}

	if !hasUpper {
		errors = append(errors, "Password must contain at least one uppercase letter")
	}
	if !hasLower {
		errors = append(errors, "Password must contain at least one lowercase letter")
	}
	if !hasNumber {
		errors = append(errors, "Password must contain at least one number")
	}
	if !hasSpecial {
		errors = append(errors, "Password must contain at least one special character")
	}

	return len(errors) == 0, errors
}

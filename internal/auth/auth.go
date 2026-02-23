package auth

import "golang.org/x/crypto/bcrypt"

type AuthService struct{}

func NewAuthService() *AuthService {
	return &AuthService{}
}

// HashPassword generates a bcrypt hash
func (s *AuthService) HashPassword(password string) (string, error) {
	bytes, err := bcrypt.GenerateFromPassword([]byte(password), 10)
	return string(bytes), err
}

// CheckPasswordHash compares password with hash
func (s *AuthService) CheckPasswordHash(password, hash string) bool {
	err := bcrypt.CompareHashAndPassword([]byte(hash), []byte(password))
	return err == nil
}

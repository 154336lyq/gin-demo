package api

import (
	"errors"
	"time"

	"github.com/golang-jwt/jwt/v5"

	"gin-demo/internal/config"
)

type Claims struct {
	Username string `json:"username"`
	UserID   string `json:"uid"`
	jwt.RegisteredClaims
}

func SignJWT(cfg *config.Config, username, userID string) (string, error) {
	if cfg.JWT.Secret == "" {
		return "", errors.New("jwt secret empty")
	}
	hours := cfg.JWT.ExpireHours
	if hours <= 0 {
		hours = 24
	}
	claims := Claims{
		Username: username,
		UserID:   userID,
		RegisteredClaims: jwt.RegisteredClaims{
			ExpiresAt: jwt.NewNumericDate(time.Now().Add(time.Duration(hours) * time.Hour)),
			IssuedAt:  jwt.NewNumericDate(time.Now()),
		},
	}
	t := jwt.NewWithClaims(jwt.SigningMethodHS256, claims)
	return t.SignedString([]byte(cfg.JWT.Secret))
}

func ParseJWT(cfg *config.Config, token string) (*Claims, error) {
	t, err := jwt.ParseWithClaims(token, &Claims{}, func(t *jwt.Token) (any, error) {
		return []byte(cfg.JWT.Secret), nil
	})
	if err != nil {
		return nil, err
	}
	if c, ok := t.Claims.(*Claims); ok && t.Valid {
		return c, nil
	}
	return nil, errors.New("invalid token")
}

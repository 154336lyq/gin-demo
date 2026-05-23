package api

import (
	"net/http"
	"strings"

	"github.com/gin-gonic/gin"

	"gin-demo/internal/config"
)

const AuthHeader = "Authorization"

// JWTMiddleware 按 Bearer Token 校验 JWT。
func JWTMiddleware(cfg *config.Config) gin.HandlerFunc {
	return func(c *gin.Context) {
		raw := c.GetHeader(AuthHeader)
		if raw == "" || !strings.HasPrefix(raw, "Bearer ") {
			c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{"error": "missing bearer token"})
			return
		}
		token := strings.TrimPrefix(raw, "Bearer ")
		cl, err := ParseJWT(cfg, token)
		if err != nil {
			c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{"error": "invalid token"})
			return
		}
		c.Set("username", cl.Username)
		c.Set("uid", cl.UserID)
		c.Next()
	}
}

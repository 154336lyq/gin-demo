package api

import (
	"net/http"

	"github.com/gin-gonic/gin"

	"gin-demo/internal/config"
	"gin-demo/internal/store"
)

type LoginReq struct {
	Username string `json:"username" binding:"required"`
	Password string `json:"password" binding:"required"`
}

func HandleLogin(cfg *config.Config, users *store.UserStore) gin.HandlerFunc {
	return func(c *gin.Context) {
		var req LoginReq
		if err := c.ShouldBindJSON(&req); err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
			return
		}
		u, ok := users.Authenticate(req.Username, req.Password)
		if !ok {
			c.JSON(http.StatusUnauthorized, gin.H{"error": "invalid credentials"})
			return
		}
		token, err := SignJWT(cfg, u.Username, u.ID)
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return
		}
		c.JSON(http.StatusOK, gin.H{"token": token, "user": u})
	}
}

func HandleListUsers(users *store.UserStore) gin.HandlerFunc {
	return func(c *gin.Context) {
		c.JSON(http.StatusOK, gin.H{"users": users.List()})
	}
}

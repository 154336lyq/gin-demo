package api

import (
	"net/http"

	"github.com/gin-gonic/gin"

	"gin-demo/internal/crawler"
)

// HandleHTTPProbe 演示「网络编程 + 简易探测」：查询远程 URL 返回状态码（不是完整爬虫，仅扩展位）。
func HandleHTTPProbe() gin.HandlerFunc {
	return func(c *gin.Context) {
		url := c.Query("url")
		code, err := crawler.HeadStatus("gin-demo-chain-backend/1.0", url)
		if err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
			return
		}
		c.JSON(http.StatusOK, gin.H{"url": url, "status": code})
	}
}

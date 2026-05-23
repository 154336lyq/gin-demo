package api

import (
	"net/http"

	"github.com/gin-gonic/gin"

	"gin-demo/internal/pipeline"
)

// HandlePipelineStats 返回并发流水线统计（演示 Mutex/原子计数产出）。
func HandlePipelineStats(bus *pipeline.Bus) gin.HandlerFunc {
	return func(c *gin.Context) {
		h, t, recent := bus.Stats()
		c.JSON(http.StatusOK, gin.H{
			"headers_processed":   h,
			"transfers_processed": t,
			"recent":              recent,
		})
	}
}

package api

import (
	"database/sql"
	"net/http"
	"strconv"
	"strings"

	"github.com/gin-gonic/gin"

	"gin-demo/internal/indexer"
)

// HandleIndexerStatus 返回链下同步进度（MySQL checkpoint + Redis 缓存后端）。
func HandleIndexerStatus(eng *indexer.Engine) gin.HandlerFunc {
	return func(c *gin.Context) {
		st, err := eng.Status(c.Request.Context())
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return
		}
		c.JSON(http.StatusOK, st)
	}
}

// HandleIndexerBlocks 从 MySQL 分页查询已同步区块。
func HandleIndexerBlocks(eng *indexer.Engine) gin.HandlerFunc {
	return func(c *gin.Context) {
		from, _ := strconv.Atoi(c.DefaultQuery("from", "0"))
		limit, _ := strconv.Atoi(c.DefaultQuery("limit", "20"))
		if limit <= 0 || limit > 100 {
			limit = 20
		}
		rows, err := eng.Store().ListBlocks(c.Request.Context(), from, limit)
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return
		}
		c.JSON(http.StatusOK, gin.H{"from": from, "limit": limit, "blocks": rows})
	}
}

// HandleIndexerBlockByNumber 优先读 Redis 缓存，miss 时读 MySQL。
func HandleIndexerBlockByNumber(eng *indexer.Engine) gin.HandlerFunc {
	return func(c *gin.Context) {
		num, err := strconv.ParseUint(c.Param("number"), 10, 64)
		if err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": "invalid block number"})
			return
		}
		if cached, err := eng.GetCachedBlock(c.Request.Context(), num); err == nil && cached != "" {
			c.JSON(http.StatusOK, gin.H{"source": "cache", "data": cached})
			return
		}
		row, err := eng.Store().GetBlockByNumber(c.Request.Context(), num)
		if err == sql.ErrNoRows {
			c.JSON(http.StatusNotFound, gin.H{"error": "block not synced yet"})
			return
		}
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return
		}
		c.JSON(http.StatusOK, gin.H{"source": "mysql", "block": row})
	}
}

// HandleIndexerTransactions 查询已同步交易（可按 block_number 过滤）。
func HandleIndexerTransactions(eng *indexer.Engine) gin.HandlerFunc {
	return func(c *gin.Context) {
		var blockNum uint64
		if raw := c.Query("block_number"); raw != "" {
			n, err := strconv.ParseUint(raw, 10, 64)
			if err != nil {
				c.JSON(http.StatusBadRequest, gin.H{"error": "invalid block_number"})
				return
			}
			blockNum = n
		}
		limit, _ := strconv.Atoi(c.DefaultQuery("limit", "50"))
		if limit <= 0 || limit > 200 {
			limit = 50
		}
		rows, err := eng.Store().ListTransactions(c.Request.Context(), blockNum, limit)
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return
		}
		c.JSON(http.StatusOK, gin.H{"block_number": blockNum, "limit": limit, "transactions": rows})
	}
}

// HandleIndexerEvents 查询通用 ABI 解析后的事件日志。
func HandleIndexerEvents(eng *indexer.Engine) gin.HandlerFunc {
	return func(c *gin.Context) {
		contract := strings.TrimSpace(c.Query("contract"))
		eventName := strings.TrimSpace(c.Query("event_name"))
		var blockNum uint64
		if raw := c.Query("block_number"); raw != "" {
			n, err := strconv.ParseUint(raw, 10, 64)
			if err != nil {
				c.JSON(http.StatusBadRequest, gin.H{"error": "invalid block_number"})
				return
			}
			blockNum = n
		}
		limit, _ := strconv.Atoi(c.DefaultQuery("limit", "50"))
		if limit <= 0 || limit > 200 {
			limit = 50
		}
		rows, err := eng.Store().ListEvents(c.Request.Context(), contract, eventName, blockNum, limit)
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return
		}
		c.JSON(http.StatusOK, gin.H{
			"contract":     contract,
			"event_name":   eventName,
			"block_number": blockNum,
			"limit":        limit,
			"events":       rows,
		})
	}
}

// HandleIndexerGapScans 返回最近漏块扫描审计记录。
func HandleIndexerGapScans(eng *indexer.Engine) gin.HandlerFunc {
	return func(c *gin.Context) {
		limit, _ := strconv.Atoi(c.DefaultQuery("limit", "10"))
		if limit <= 0 || limit > 50 {
			limit = 10
		}
		rows, _, err := eng.Store().ListRecentGapScans(c.Request.Context(), limit)
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return
		}
		c.JSON(http.StatusOK, gin.H{"limit": limit, "scans": rows})
	}
}

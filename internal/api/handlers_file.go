package api

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/gin-gonic/gin"

	"gin-demo/internal/config"
)

var uploadIndex sync.Map // sha256 hex (64) -> stored filename

func isSHA256Hex(s string) bool {
	if len(s) != 64 {
		return false
	}
	for i := 0; i < 64; i++ {
		c := s[i]
		switch {
		case c >= '0' && c <= '9', c >= 'a' && c <= 'f':
		default:
			return false
		}
	}
	return true
}

func downloadBySHAPath(shaHex string) string {
	return "/api/v1/files/by-sha/" + shaHex
}

func absoluteDownloadBySHAURL(cfg *config.Config, c *gin.Context, shaHex string) string {
	rel := downloadBySHAPath(shaHex)
	if base := strings.TrimSpace(cfg.Server.PublicBaseURL); base != "" {
		return strings.TrimSuffix(base, "/") + rel
	}
	scheme := "http"
	if c.Request.TLS != nil {
		scheme = "https"
	}
	if xfp := strings.TrimSpace(c.GetHeader("X-Forwarded-Proto")); xfp != "" {
		scheme = xfp
	}
	return scheme + "://" + c.Request.Host + rel
}

// HandleUpload 演示 multipart 上传 + SHA-256；响应含可直接存库、无需编码中文路径的 download_url。
func HandleUpload(cfg *config.Config) gin.HandlerFunc {
	return func(c *gin.Context) {
		if err := os.MkdirAll(cfg.Files.UploadDir, 0o755); err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return
		}
		fh, err := c.FormFile("file")
		if err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": "need form field file"})
			return
		}
		src, err := fh.Open()
		if err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
			return
		}
		defer src.Close()

		name := fmt.Sprintf("%d_%s", time.Now().UnixNano(), filepath.Base(fh.Filename))
		dstPath := filepath.Join(cfg.Files.UploadDir, name)
		dst, err := os.Create(dstPath)
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return
		}
		defer dst.Close()

		h := sha256.New()
		if _, err := io.Copy(io.MultiWriter(dst, h), src); err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return
		}
		sum := hex.EncodeToString(h.Sum(nil))
		uploadIndex.Store(sum, name)
		c.JSON(http.StatusOK, gin.H{
			"stored_as":     name,
			"sha256":        sum,
			"size":          fh.Size,
			"download_path": downloadBySHAPath(sum),
			"download_url":  absoluteDownloadBySHAURL(cfg, c, sum),
		})
	}
}

// HandleDownloadBySHA 按 SHA-256 下载（路径仅 [0-9a-f]，避免中文出现在 URL path）。
func HandleDownloadBySHA(cfg *config.Config) gin.HandlerFunc {
	return func(c *gin.Context) {
		sha := strings.TrimSpace(c.Param("sha"))
		sha = strings.TrimPrefix(strings.ToLower(sha), "0x")
		if !isSHA256Hex(sha) {
			c.JSON(http.StatusBadRequest, gin.H{"error": "invalid sha256"})
			return
		}
		v, ok := uploadIndex.Load(sha)
		if !ok {
			c.JSON(http.StatusNotFound, gin.H{"error": "unknown file or server restarted (index in memory only)"})
			return
		}
		stored, _ := v.(string)
		baseName := filepath.Base(stored)
		path := filepath.Join(cfg.Files.UploadDir, baseName)
		if _, err := os.Stat(path); err != nil {
			c.JSON(http.StatusNotFound, gin.H{"error": "file missing on disk"})
			return
		}
		c.FileAttachment(path, baseName)
	}
}

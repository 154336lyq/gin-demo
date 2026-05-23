// Package crawler 提供最小「拉取 URL 状态码」能力，作为网络编程 / 简易爬虫入门的扩展点。
// 生产级爬虫需加入：robots.txt、速率限制、User-Agent、HTML 解析与去重等。
package crawler

import (
	"fmt"
	"net/http"
	"time"
)

// HeadStatus 对目标 URL 发起 GET（可改为只读请求受站点限制时需谨慎），返回 HTTP 状态码。
// 仅用于教学：请勿对未授权目标高频请求。
func HeadStatus(userAgent, url string) (int, error) {
	if url == "" {
		return 0, fmt.Errorf("empty url")
	}
	client := &http.Client{Timeout: 10 * time.Second}
	req, err := http.NewRequest(http.MethodGet, url, nil)
	if err != nil {
		return 0, err
	}
	if userAgent != "" {
		req.Header.Set("User-Agent", userAgent)
	}
	resp, err := client.Do(req)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()
	return resp.StatusCode, nil
}

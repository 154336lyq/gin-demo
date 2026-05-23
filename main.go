package main

import (
	"net/http"
	"strconv"

	"github.com/gin-gonic/gin"
)

type User struct {
	ID   int    `json:"id"`
	Name string `json:"name"`
	Age  int    `json:"age"`
}

func main() {
	// 创建一个默认的 Gin 路由引擎
	r := gin.Default()

	// 模拟数据库（内存存储）
	users := map[int]User{
		1: {ID: 1, Name: "Tom", Age: 20},
		2: {ID: 2, Name: "Jerry", Age: 22},
	}
	nextID := 3

	// 基础健康检查接口
	r.GET("/", func(c *gin.Context) {
		c.JSON(http.StatusOK, gin.H{
			"message": "Gin 服务运行正常",
		})
	})

	// GET: 列表查询（query 参数）
	// 示例: /users?min_age=21
	r.GET("/users", func(c *gin.Context) {
		minAgeStr := c.DefaultQuery("min_age", "0")
		minAge, err := strconv.Atoi(minAgeStr)
		if err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": "min_age 必须是数字"})
			return
		}

		result := make([]User, 0)
		for _, u := range users {
			if u.Age >= minAge {
				result = append(result, u)
			}
		}
		c.JSON(http.StatusOK, gin.H{"data": result})
	})

	// GET: 单条查询（path 参数）
	r.GET("/users/:id", func(c *gin.Context) {
		id, err := strconv.Atoi(c.Param("id"))
		if err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": "id 必须是数字"})
			return
		}

		u, ok := users[id]
		if !ok {
			c.JSON(http.StatusNotFound, gin.H{"error": "用户不存在"})
			return
		}
		c.JSON(http.StatusOK, gin.H{"data": u})
	})

	// POST: 创建（JSON body）
	r.POST("/users", func(c *gin.Context) {
		var req struct {
			Name string `json:"name" binding:"required"`
			Age  int    `json:"age" binding:"required"`
		}
		if err := c.ShouldBindJSON(&req); err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
			return
		}

		user := User{
			ID:   nextID,
			Name: req.Name,
			Age:  req.Age,
		}
		users[nextID] = user
		nextID++

		c.JSON(http.StatusCreated, gin.H{"message": "创建成功", "data": user})
	})

	// PUT: 全量更新（JSON body）
	r.PUT("/users/:id", func(c *gin.Context) {
		id, err := strconv.Atoi(c.Param("id"))
		if err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": "id 必须是数字"})
			return
		}

		_, ok := users[id]
		if !ok {
			c.JSON(http.StatusNotFound, gin.H{"error": "用户不存在"})
			return
		}

		var req struct {
			Name string `json:"name" binding:"required"`
			Age  int    `json:"age" binding:"required"`
		}
		if err := c.ShouldBindJSON(&req); err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
			return
		}

		updated := User{
			ID:   id,
			Name: req.Name,
			Age:  req.Age,
		}
		users[id] = updated
		c.JSON(http.StatusOK, gin.H{"message": "更新成功", "data": updated})
	})

	// DELETE: 删除（path 参数）
	r.DELETE("/users/:id", func(c *gin.Context) {
		id, err := strconv.Atoi(c.Param("id"))
		if err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": "id 必须是数字"})
			return
		}

		if _, ok := users[id]; !ok {
			c.JSON(http.StatusNotFound, gin.H{"error": "用户不存在"})
			return
		}

		delete(users, id)
		c.JSON(http.StatusOK, gin.H{"message": "删除成功"})
	})

	// 启动服务，监听 8080 端口
	r.Run(":8080")
}

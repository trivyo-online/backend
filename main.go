package main

import (
	"time"
	"trivyo_online/logicforuser"
	"trivyo_online/routes"

	"github.com/gin-contrib/cors"
	"github.com/gin-gonic/gin"
)

func main() {

	r := gin.Default()

	r.Use(cors.New(cors.Config{
		AllowOrigins:     []string{"*"}, // or your frontend URL e.g. "http://localhost:3000"
		AllowMethods:     []string{"GET", "POST", "PUT", "PATCH", "DELETE", "OPTIONS"},
		AllowHeaders:     []string{"Origin", "Content-Type", "Authorization"},
		ExposeHeaders:    []string{"Content-Length"},
		AllowCredentials: true,
		MaxAge:           12 * time.Hour,
	}))
	hub := logicforuser.NewHub()
	go hub.Run()

	// mount websocket endpoint with gin
	r.GET("/ws", func(c *gin.Context) {
		logicforuser.ServeWS(hub, c.Writer, c.Request)
	})

	// example HTTP API for broadcasting
	r.POST("/broad", func(c *gin.Context) {
		var body struct {
			Question string `json:"question"`
		}
		if err := c.BindJSON(&body); err != nil {
			c.JSON(400, gin.H{"error": "invalid body"})
			return
		}
		logicforuser.BroadcastQuestion(hub, body.Question)
		c.JSON(200, gin.H{"status": "sent"})
	})

	r.Run(":8080")

	routes.RegisterRoutes(r)

	r.Run(":8000")
}

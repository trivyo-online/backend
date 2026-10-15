package main

import (
	"log"
	"os"
	"strings"
	"time"

	"trivyo_online/routes"
	"trivyo_online/structure"

	"github.com/gin-contrib/cors"
	"github.com/gin-gonic/gin"
)

func main() {
	// Connect up front so a bad MON fails at startup, not on first request.
	structure.Client()
	structure.RedisClient()

	r := gin.Default()

	// CORS_ORIGINS is a comma-separated allowlist, e.g.
	// "http://localhost:5173,https://trivyo.example". Unset allows any origin.
	// Auth uses bearer tokens, not cookies, so credentials are not needed.
	corsCfg := cors.Config{
		AllowMethods:  []string{"GET", "POST", "PUT", "PATCH", "DELETE", "OPTIONS"},
		AllowHeaders:  []string{"Origin", "Content-Type", "Authorization"},
		ExposeHeaders: []string{"Content-Length"},
		MaxAge:        12 * time.Hour,
	}
	if origins := os.Getenv("CORS_ORIGINS"); origins != "" {
		for _, o := range strings.Split(origins, ",") {
			corsCfg.AllowOrigins = append(corsCfg.AllowOrigins, strings.TrimSpace(o))
		}
	} else {
		corsCfg.AllowAllOrigins = true
	}
	r.Use(cors.New(corsCfg))

	routes.RegisterRoutes(r)

	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
	}
	log.Println("listening on :" + port)
	if err := r.Run(":" + port); err != nil {
		log.Fatal(err)
	}
}

package main

import (
	"log"
	"os"
	"strings"
	"time"

	"trivyo_online/logicforuser"
	"trivyo_online/routes"
	"trivyo_online/structure"

	"github.com/gin-contrib/cors"
	"github.com/gin-contrib/gzip"
	"github.com/gin-gonic/gin"
)

func main() {
	// Connect up front so a bad MON fails at startup, not on first request.
	structure.Client()
	structure.RedisClient()
	logicforuser.StartScheduler()

	r := gin.Default()

	// CORS_ORIGINS is a comma-separated allowlist, e.g.
	// "https://trivyo.vercel.app,https://trivyo-*.vercel.app". Unset allows
	// any origin. Auth uses bearer tokens, not cookies, so credentials are
	// not needed.
	corsCfg := cors.Config{
		AllowMethods:  []string{"GET", "POST", "PUT", "PATCH", "DELETE", "OPTIONS"},
		AllowHeaders:  []string{"Origin", "Content-Type", "Authorization"},
		ExposeHeaders: []string{"Content-Length"},
		MaxAge:        12 * time.Hour,
	}
	if allow := parseOrigins(os.Getenv("CORS_ORIGINS")); len(allow) > 0 {
		log.Printf("CORS: allowing %s", strings.Join(allow, ", "))
		corsCfg.AllowOriginFunc = func(origin string) bool {
			if originAllowed(allow, origin) {
				return true
			}
			log.Printf("CORS: blocked origin %q (not in CORS_ORIGINS)", origin)
			return false
		}
	} else {
		corsCfg.AllowAllOrigins = true
	}
	r.Use(cors.New(corsCfg))
	// Compress JSON for slow links; WebSockets negotiate their own deflate.
	r.Use(gzip.Gzip(gzip.DefaultCompression, gzip.WithExcludedPaths([]string{"/api/host", "/api/connect"})))

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

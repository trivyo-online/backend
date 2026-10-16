package routes

import (
	"trivyo_online/auth"
	"trivyo_online/logicforadmin"
	"trivyo_online/logicforuser"

	"github.com/gin-gonic/gin"
)

func RegisterRoutes(r *gin.Engine) {
	api := r.Group("/api")
	{
		api.GET("/health", func(c *gin.Context) { c.JSON(200, gin.H{"status": "ok"}) })

		// Admin accounts
		api.POST("/admin/signup", auth.Signup)
		api.POST("/admin/login", auth.Login)

		admin := api.Group("", auth.Require())
		admin.GET("/admin/me", auth.Me)
		admin.POST("/create-event", logicforadmin.InsertEvent)
		admin.POST("/add-question", logicforadmin.AddQuestionHandler)
		admin.GET("/admin/events", logicforadmin.ListEvents)
		admin.GET("/admin/events/:roomid", logicforadmin.GetEvent)
		admin.PUT("/admin/events/:roomid", logicforadmin.UpdateEvent)
		admin.DELETE("/admin/events/:roomid", logicforadmin.DeleteEvent)
		admin.POST("/admin/events/:roomid/reset", logicforadmin.ResetEvent)

		// Rankings (public)
		api.GET("/leaderboard", logicforuser.GlobalLeaderboard)
		api.GET("/rooms", logicforuser.RecentRooms)
		api.GET("/rooms/:roomid", logicforuser.RoomInfo)
		api.GET("/rooms/:roomid/leaderboard", logicforuser.RoomLeaderboard)
		api.GET("/rooms/:roomid/stats", logicforuser.RoomStats)

		// WebSockets: the host authenticates with ?token=, players join by room code.
		api.GET("/host", logicforuser.HostConnection)
		api.GET("/connect", logicforuser.ConnecttoRoom)
	}
}

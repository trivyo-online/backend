package routes

import (
	"trivyo_online/logicforadmin"
	"trivyo_online/logicforuser"

	"github.com/gin-gonic/gin"
)

func RegisterRoutes(r *gin.Engine) {
	// You can group your routes logically

	api := r.Group("/api")
	{
		api.POST("/create-event", logicforadmin.InsertEvent)
		api.POST("/add-question", logicforadmin.AddQuestionHandler)
		// api.GET("/ws", func(c *gin.Context) {
		// 	name := c.Query("name")
		// 	logicforuser.ServeWS(hub, c.Writer, c.Request, name)
		// })
		// api.POST("/broad", func(c *gin.Context) {
		// 	var body struct {
		// 		Question string `json:"question"`
		// 	}
		// 	if err := c.BindJSON(&body); err != nil {
		// 		c.JSON(400, gin.H{"error": "invalid body"})
		// 		return
		// 	}
		// 	logicforuser.BroadcastQuestion(hub, body.Question)
		// 	c.JSON(200, gin.H{"status": "sent"})
		// })

		api.GET("/host", logicforuser.HostConnection)
		api.GET("/connect", logicforuser.ConnecttoRoom)

	}
}

package routes

import (
	"trivyo_online/logicforadmin"

	"github.com/gin-gonic/gin"
)

func RegisterRoutes(r *gin.Engine) {
	// You can group your routes logically
	api := r.Group("/api")
	{
		api.POST("/create-event", logicforadmin.InsertEvent)
		api.POST("/add-question", logicforadmin.AddQuestionHandler)

	}
}

package logicforadmin

import (
	"context"
	"log"
	"math/rand"
	"net/http"
	"time"

	"trivyo_online/structure"

	"github.com/gin-gonic/gin"
	"go.mongodb.org/mongo-driver/bson"
)

type Event struct {
	Nameofroom string
	StartTime  string
	DateofQuiz string
	RoomId     string
	Questions  []structure.Creataquestion
}

var client = structure.Client()

func generateRandomCode(n int) string {
	const letters = "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
	code := make([]byte, n)
	for i := range code {
		code[i] = letters[rand.Intn(len(letters))]
	}
	return string(code)
}

func InsertEvent(c *gin.Context) {
	var newEvent structure.Createroom
	db := client.Database("quizdb")
	collection := db.Collection("event")
	if err := c.BindJSON(&newEvent); err != nil {
		c.JSON(400, gin.H{"error": "Invalid JSON"})
		return
	}

	event := Event{
		Nameofroom: newEvent.Nameofroom,
		StartTime:  newEvent.StartTime,
		DateofQuiz: newEvent.DateofQuiz,
		RoomId:     generateRandomCode(8),
		Questions:  []structure.Creataquestion{},
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	result, err := collection.InsertOne(ctx, event)
	if err != nil {
		log.Println("Insert error:", err)
		c.JSON(500, gin.H{"error": "Failed to insert"})
		return
	}

	c.JSON(200, gin.H{"message": "Event inserted", "id": result.InsertedID})
}

func AddQuestionHandler(c *gin.Context) {
	var req struct {
		RoomId   string                   `json:"roomid"`
		Question structure.Creataquestion `json:"question"`
	}

	if err := c.BindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid JSON"})
		return
	}

	db := structure.Client().Database("quizdb")
	collection := db.Collection("event")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	filter := bson.M{"roomid": req.RoomId}
	update := bson.M{"$push": bson.M{"questions": req.Question}}

	result, err := collection.UpdateOne(ctx, filter, update)
	if err != nil {
		log.Println("Update error:", err)
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to add question"})
		return
	}

	if result.MatchedCount == 0 {
		c.JSON(http.StatusNotFound, gin.H{"error": "Event not found"})
		return
	}

	c.JSON(http.StatusOK, gin.H{"message": "Question added"})
}

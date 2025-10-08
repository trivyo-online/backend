package structure

import (
	"context"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/joho/godotenv"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type Createroom struct {
	Nameofroom string
	StartTime  string
	DateofQuiz string
}

type Creataquestion struct {
	Question         string
	Options          []string
	Correctoption    string
	Eachquestiontime *int
}

func Client() *mongo.Client {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	err := godotenv.Load()
	if err != nil {
		log.Println("⚠️ Warning: .env file not found, make sure it exists")
	}
	mongourl := os.Getenv("MON")
	fmt.Println("url ", mongourl)
	client, err := mongo.Connect(ctx, options.Client().ApplyURI(mongourl))
	if err != nil {
		log.Fatal("X Mongo connect error", err)

	}
	if err := client.Ping(ctx, nil); err != nil {
		log.Fatal("Mongo ping error:", err)
	}

	fmt.Println("CONNECTED SUCCESFULLY")

	return client
}

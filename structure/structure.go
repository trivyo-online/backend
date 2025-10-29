package structure

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"os"
	"sync"
	"time"

	"github.com/gorilla/websocket"
	"github.com/joho/godotenv"
	"github.com/redis/go-redis/v9"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type CreateEvent struct {
	Nameofroom string
	StartTime  string
	DateofQuiz string
	HostID     string
}

type Question struct {
	Question         string
	Options          []string
	Correctoption    string
	Eachquestiontime *int
}
type Event struct {
	Nameofroom        string `bson:"nameofroom"`
	StartTime         string `bson:"starttime"`
	DateofQuiz        string `bson:"dateofquiz"`
	RoomId            string `bson:"roomid"`
	HostID            string
	Questions         []Question `bson:"questions"`
	QuizStarted       bool
	Timingperquestion time.Duration
}

type ClientMessage struct {
	Type       string `json:"type"`
	UserID     string `json:"user_id"`
	RoomID     string `json:"room_id"`
	QuestionID string `json:"question_id,omitempty"`
	Selected   int    `json:"selected,omitempty"`
}

type ServerMessage struct {
	Type     string    `json:"type"`
	Message  string    `json:"message,omitempty"`
	Question *Question `json:"question,omitempty"`
	Correct  *bool     `json:"correct,omitempty"`
	Score    *int      `json:"score,omitempty"`
}
type CreateRoomRequest struct {
	RoomID         string   `json:"room_id,omitempty"`
	HostID         string   `json:"host_id,omitempty"`
	QuestionIDs    []string `json:"question_ids,omitempty"`
	QuestionTopic  string   `json:"question_topic,omitempty"`
	NumQuestions   int      `json:"num_questions,omitempty"`
	QuestionTTLMin int      `json:"question_ttl_minutes,omitempty"`
}

type HostConnection struct {
	HostID string
	RoomId string
}
type WClient struct {
	Conn   *websocket.Conn
	Send   chan []byte
	UserID string
	RoomID string
}

type HWClient struct {
	Conn   *websocket.Conn
	Send   chan []byte
	HostID string
	RoomID string
}

type RoomManager struct {
	Rooms map[string]*Room
	Mutex sync.RWMutex
}

var Mgr = &RoomManager{Rooms: make(map[string]*Room)}

type Room struct {
	ID           string
	HostID       string
	Clients      map[*WClient]bool
	Host         map[*HWClient]bool
	Register     chan *WClient
	Unregister   chan *WClient
	Broadcast    chan []byte
	QuestionsKey string
	QuizStarted  bool
	CurrentIndex int
	Scores       map[string]int
	Mutex        sync.RWMutex
	QuestionTTL  time.Duration
}

var Upgrader = websocket.Upgrader{
	CheckOrigin: func(r *http.Request) bool { return true },
}

func Client() *mongo.Client {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	err := godotenv.Load()
	if err != nil {
		log.Println("Warning: .env file not found, make sure it exists")
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

var ctx = context.Background()

func RedisClient() *redis.Client {
	rdb := redis.NewClient(&redis.Options{
		Addr:     "redis-13071.c9.us-east-1-2.ec2.redns.redis-cloud.com:13071",
		Username: "default",
		Password: "JF4ZnoE683gQi04bdanJoTRR80pvXnhP",
		DB:       0,
	})

	if pong, err := rdb.Ping(ctx).Result(); err != nil {
		panic(err)
	} else {
		fmt.Println("Redis connected:", pong)
	}

	return rdb
}

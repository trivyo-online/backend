package structure

import (
	"context"
	"log"
	"net/http"
	"os"
	"sync"
	"time"

	"github.com/gorilla/websocket"
	"github.com/joho/godotenv"
	"github.com/redis/go-redis/v9"
	"go.mongodb.org/mongo-driver/bson"
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
	Nameofroom        string     `bson:"nameofroom"`
	StartTime         string     `bson:"starttime"`
	DateofQuiz        string     `bson:"dateofquiz"`
	RoomId            string     `bson:"roomid"`
	HostID            string     `bson:"hostid"`
	Questions         []Question `bson:"questions"`
	QuizStarted       bool       `bson:"quizstarted"`
	Timingperquestion time.Duration
	CreatedAt         time.Time `bson:"createdat"`
}

// Answer is one graded submission. (roomid, questionno, userid) is unique,
// so each player scores a question at most once.
type Answer struct {
	RoomID     string    `bson:"roomid" json:"roomid"`
	UserID     string    `bson:"userid" json:"userid"`
	QuestionNo int       `bson:"questionno" json:"questionno"`
	Choice     int       `bson:"choice" json:"choice"`
	Correct    bool      `bson:"correct" json:"correct"`
	Points     int       `bson:"points" json:"points"`
	At         time.Time `bson:"at" json:"at"`
	Ms         int64     `bson:"ms" json:"ms"` // time from question open to answer
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

var Upgrader = websocket.Upgrader{
	CheckOrigin: func(r *http.Request) bool { return true },
}

func init() {
	if err := godotenv.Load(); err != nil {
		log.Println("Warning: .env file not found, using process environment")
	}
}

var (
	mongoOnce   sync.Once
	mongoClient *mongo.Client
)

// Client returns the shared Mongo client, connecting on first use.
// The connection string comes from MON (e.g. mongodb://localhost:27017).
func Client() *mongo.Client {
	mongoOnce.Do(func() {
		uri := os.Getenv("MON")
		if uri == "" {
			log.Fatal("MON is not set: put your MongoDB connection string in .env (see .env.example)")
		}
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		c, err := mongo.Connect(ctx, options.Client().ApplyURI(uri))
		if err != nil {
			log.Fatal("Mongo connect error: ", err)
		}
		if err := c.Ping(ctx, nil); err != nil {
			log.Fatal("Mongo ping error: ", err)
		}
		log.Println("Mongo connected")
		mongoClient = c
		ensureIndexes(c)
	})
	return mongoClient
}

func DB() *mongo.Database {
	return Client().Database("quizdb")
}

func ensureIndexes(c *mongo.Client) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	db := c.Database("quizdb")
	idx := []struct {
		coll  string
		model mongo.IndexModel
	}{
		{"event", mongo.IndexModel{Keys: bson.D{{Key: "roomid", Value: 1}}, Options: options.Index().SetUnique(true)}},
		{"admins", mongo.IndexModel{Keys: bson.D{{Key: "username", Value: 1}}, Options: options.Index().SetUnique(true)}},
		{"answers", mongo.IndexModel{Keys: bson.D{{Key: "roomid", Value: 1}, {Key: "questionno", Value: 1}, {Key: "userid", Value: 1}}, Options: options.Index().SetUnique(true)}},
		{"answers", mongo.IndexModel{Keys: bson.D{{Key: "userid", Value: 1}}}},
	}
	for _, i := range idx {
		if _, err := db.Collection(i.coll).Indexes().CreateOne(ctx, i.model); err != nil {
			log.Printf("index on %s: %v", i.coll, err)
		}
	}
}

var (
	redisOnce   sync.Once
	redisClient *redis.Client
)

// RedisClient returns the shared Redis client, or nil when REDIS_ADDR is
// unset or unreachable. Redis is only a cache for live events, so callers
// must handle nil.
func RedisClient() *redis.Client {
	redisOnce.Do(func() {
		addr := os.Getenv("REDIS_ADDR")
		if addr == "" {
			log.Println("REDIS_ADDR not set: running without the Redis event cache")
			return
		}
		rdb := redis.NewClient(&redis.Options{
			Addr:     addr,
			Username: os.Getenv("REDIS_USERNAME"),
			Password: os.Getenv("REDIS_PASSWORD"),
		})
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := rdb.Ping(ctx).Err(); err != nil {
			log.Println("Redis unreachable, continuing without cache:", err)
			return
		}
		log.Println("Redis connected")
		redisClient = rdb
	})
	return redisClient
}

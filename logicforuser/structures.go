package logicforuser

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"strconv"
	"strings"
	"sync"
	"time"
	"trivyo_online/structure"

	"github.com/gorilla/websocket"
)

// type Client struct {
// 	Conn  *websocket.Conn
// 	Send  chan []byte
// 	Id    string
// 	Scroe int
// 	Name  string
// }

type Answer struct {
	ClientID string
	Choice   string
}

type UserClient struct {
	Userid string
	Conn   *websocket.Conn
	Send   chan []byte
	RoomID string
}

type HostClient struct {
	Priv   *string
	Conn   *websocket.Conn
	Send   chan []byte
	HostID string
	RoomID string
}

type Room struct {
	ID             string
	HostID         string
	Clients        map[*UserClient]bool
	Hosts          map[*HostClient]bool
	Register       chan *UserClient
	HostRegister   chan *HostClient
	Hostunregister chan *HostClient
	Unregister     chan *UserClient
	Broadcast      chan []byte
	HostBrodcast   chan []byte
	QuizStarted    bool
	CurrentIndex   int
	Scores         map[string]int
	Mutex          sync.RWMutex
	QuestionTTL    time.Duration
}

//	type Hub struct {
//		Id         string
//		Clients    map[*Client]bool
//		Broadcast  chan []byte
//		Register   chan *structure.HWClient
//		Unregister chan *structure.HWClient
//		Answers    chan Answer
//	}
func NewRoom(id string, hostID string, ttl time.Duration) *Room {
	return &Room{
		ID:             id,
		HostID:         hostID,
		Clients:        make(map[*UserClient]bool),
		Hosts:          make(map[*HostClient]bool),
		Register:       make(chan *UserClient, 256),
		HostRegister:   make(chan *HostClient, 256),
		Hostunregister: make(chan *HostClient, 256),
		Unregister:     make(chan *UserClient, 256),
		Broadcast:      make(chan []byte, 256),
		HostBrodcast:   make(chan []byte, 256),
		QuizStarted:    false,
		CurrentIndex:   0,
		Scores:         make(map[string]int),
		QuestionTTL:    ttl,
	}
}

type Ifpresentupgradeconnection struct {
	UserId string
	RoomID string
	HostID string
}

// func NewHub() *Hub {
// 	return &Hub{
// 		Clients:    make(map[*Client]bool),
// 		Broadcast:  make(chan []byte),
// 		Register:   make(chan *structure.HWClient),
// 		Unregister: make(chan *structure.HWClient),
// 		Answers:    make(chan Answer),
// 	}
// }

func (c *HostClient) HWritePump() {
	for msg := range c.Send {
		err := c.Conn.WriteMessage(websocket.TextMessage, msg)
		if err != nil {
			break
		}
	}
	c.Conn.Close()
}

func (c *UserClient) UWritePump() {
	for msg := range c.Send {
		err := c.Conn.WriteMessage(websocket.TextMessage, msg)
		if err != nil {
			break
		}
	}
	c.Conn.Close()
}
func (c *UserClient) UReadPump(hub *Room) {
	defer func() {
		hub.Unregister <- c
		c.Conn.Close()
	}()

	for {
		_, msg, err := c.Conn.ReadMessage()
		if err != nil {
			log.Println("User read error:", err)
			break
		}
		log.Println("User sent:", string(msg))

		hub.HostBrodcast <- msg
	}
}

type HostMessage struct {
	RoomID     string `json:"roomid"`
	UserID     string `json:"userid"`
	QuestionNo string `json:"questionno"`
	Ans        string `json:"ans"`
}

var ctx = context.Background()

func (c *HostClient) HReadPump(hub *Room) {
	defer func() {
		hub.Hostunregister <- c
		c.Conn.WriteMessage(websocket.CloseMessage,
			websocket.FormatCloseMessage(websocket.CloseNormalClosure, "connection closed"))
		c.Conn.Close()
	}()

	rdb := structure.RedisClient()

	for {
		_, msg, err := c.Conn.ReadMessage()
		if err != nil {
			log.Println("Host read error:", err)
			break
		}

		var data HostMessage
		if err := json.Unmarshal(msg, &data); err != nil {
			log.Println("Invalid JSON:", err)
			continue
		}

		roomKey := fmt.Sprintf("event:%s", data.UserID)
		roomData, err := rdb.Do(ctx, "JSON.GET", roomKey).Text()
		if err != nil {
			log.Println("Redis JSON.GET error:", err)
			continue
		}

		var quiz struct {
			Questions []struct {
				Question         string      `json:"Question"`
				Options          []string    `json:"Options"`
				Correctoption    string      `json:"Correctoption"`
				Eachquestiontime interface{} `json:"Eachquestiontime"`
			} `json:"Questions"`
		}
		if err := json.Unmarshal([]byte(roomData), &quiz); err != nil {
			log.Println("Quiz unmarshal error:", err)
			continue
		}

		qNo, err := strconv.Atoi(data.QuestionNo)
		if err != nil {
			log.Println("Invalid question number format:", data.QuestionNo)
			continue
		}
		if qNo < 0 || qNo >= len(quiz.Questions) {
			log.Println("Invalid question number:", data.QuestionNo)
			continue
		}

		correctIndex, err := strconv.Atoi(quiz.Questions[qNo].Correctoption)
		if err != nil || correctIndex < 0 || correctIndex >= len(quiz.Questions[qNo].Options) {
			log.Println("Invalid correct option index for question:", qNo)
			continue
		}

		correctAns := quiz.Questions[qNo].Options[correctIndex]
		log.Printf("User %s answered: %s | Correct: %s", data.UserID, data.Ans, correctAns)

		if strings.EqualFold(data.Ans, correctAns) {
			scoreKey := fmt.Sprintf("score:%s:%s", data.RoomID, data.UserID)
			log.Printf("Answer matched! Writing score for key: %s", scoreKey)

			if _, err := rdb.IncrBy(ctx, scoreKey, 50).Result(); err != nil {
				log.Println("Redis score increment error:", err)
			} else {
				log.Printf("✅ User %s +50 points!", data.UserID)
			}
		} else {
			log.Printf("❌ Wrong answer by %s", data.UserID)
		}
	}
}

var Rooms = make(map[string]*Room)
var roomMutex sync.RWMutex

func SaveRoom(r *Room) {
	roomMutex.Lock()
	defer roomMutex.Unlock()
	Rooms[r.ID] = r
}

func GetRoom(id string) *Room {
	roomMutex.RLock()
	defer roomMutex.RUnlock()
	return Rooms[id]
}

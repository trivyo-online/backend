package logicforuser

import (
	"log"
	"sync"
	"time"

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

func (c *HostClient) HReadPump(hub *Room) {
	defer func() {
		hub.Hostunregister <- c
		c.Conn.Close()
	}()

	for {
		_, msg, err := c.Conn.ReadMessage()
		if err != nil {
			log.Println("Host read error:", err)
			break
		}
		log.Println("Host sent:", string(msg))
		hub.Broadcast <- msg
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

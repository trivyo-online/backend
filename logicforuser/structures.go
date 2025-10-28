package logicforuser

import (
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
		Register:       make(chan *UserClient),
		HostRegister:   make(chan *HostClient),
		Hostunregister: make(chan *HostClient),
		Unregister:     make(chan *UserClient),
		Broadcast:      make(chan []byte),
		HostBrodcast:   make(chan []byte),
		QuizStarted:    false,
		CurrentIndex:   0,
		Scores:         make(map[string]int),
		QuestionTTL:    ttl,
	}
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

func (c *HostClient) WritePump() {
	for msg := range c.Send {
		err := c.Conn.WriteMessage(websocket.TextMessage, msg)
		if err != nil {
			break
		}
	}
	c.Conn.Close()
}

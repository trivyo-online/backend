package logicforuser

import "github.com/gorilla/websocket"

type Client struct {
	Conn  *websocket.Conn
	Send  chan []byte
	Id    string
	Scroe int
}

type Answer struct {
	ClientID string
	Choice   string
}

type Hub struct {
	Clients    map[*Client]bool
	Broadcast  chan []byte
	Register   chan *Client
	Unregister chan *Client
	Answers    chan Answer // collect answers from clients
}

func NewHub() *Hub {
	return &Hub{
		Clients:    make(map[*Client]bool),
		Broadcast:  make(chan []byte),
		Register:   make(chan *Client),
		Unregister: make(chan *Client),
		Answers:    make(chan Answer),
	}
}

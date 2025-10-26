package logicforuser

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"trivyo_online/logicforadmin"

	"github.com/gorilla/websocket"
)

func (h *Hub) Run() {
	for {
		select {
		case client := <-h.Register:
			h.Clients[client] = true

		case client := <-h.Unregister:
			if _, ok := h.Clients[client]; ok {
				delete(h.Clients, client)
				close(client.Send)
			}

		case message := <-h.Broadcast:
			for client := range h.Clients {
				select {
				case client.Send <- message:
				default:
					close(client.Send)
					delete(h.Clients, client)
				}
			}

		case ans := <-h.Answers:
			fmt.Printf("Client %s answered %s\n", ans.ClientID, ans.Choice)
		}
	}
}

var U = websocket.Upgrader{
	CheckOrigin: func(r *http.Request) bool { return true },
}

func ServeWS(hub *Hub, w http.ResponseWriter, r *http.Request) {
	// Upgrade HTTP connection to WebSocket
	conn, err := U.Upgrade(w, r, nil)
	if err != nil {
		log.Println("WebSocket Upgrade error:", err)
		return
	}

	// Create a new client
	client := &Client{
		Conn: conn,
		Send: make(chan []byte, 256),              // buffered channel to avoid blocking
		Id:   logicforadmin.GenerateRandomCode(6), // generate unique 6-char ID
	}

	// Register the client in the hub
	hub.Register <- client

	// Start goroutines to handle incoming and outgoing messages
	go client.WritePump()   // sends messages to the client
	go client.ReadPump(hub) // reads messages (answers) from the client
}

func (c *Client) ReadPump(hub *Hub) {
	defer func() {
		hub.Unregister <- c
		c.Conn.Close()
	}()

	for {
		_, msg, err := c.Conn.ReadMessage()
		if err != nil {
			break
		}
		// Assume client sends JSON: {"choice": "A"}
		var ans struct{ Choice string }
		json.Unmarshal(msg, &ans)

		hub.Answers <- Answer{ClientID: c.Id, Choice: ans.Choice}
	}
}

func (c *Client) WritePump() {
	for msg := range c.Send {
		err := c.Conn.WriteMessage(websocket.TextMessage, msg)
		if err != nil {
			break
		}
	}
	c.Conn.Close()
}

func BroadcastQuestion(hub *Hub, question string) {
	hub.Broadcast <- []byte(question)
}

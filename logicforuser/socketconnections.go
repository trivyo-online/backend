package logicforuser

import (
	"context"
	"log"
	"net/http"
	"time"
	"trivyo_online/structure"

	"github.com/gin-gonic/gin"
	"github.com/gorilla/websocket"
	"go.mongodb.org/mongo-driver/bson"
)

var U = websocket.Upgrader{
	CheckOrigin: func(r *http.Request) bool { return true },
}

// func (c *Client) ReadPump(hub *Hub) {
// 	defer func() {
// 		hub.Unregister <- c
// 		c.Conn.Close()
// 	}()

// 	for {
// 		_, msg, err := c.Conn.ReadMessage()
// 		if err != nil {
// 			break
// 		}
// 		// Assume client sends JSON: {"choice": "A"}
// 		var ans struct{ Choice string }
// 		json.Unmarshal(msg, &ans)

// 		hub.Answers <- Answer{ClientID: c.Id, Choice: ans.Choice}
// 	}
// }

// func (c *HWClient) WritePump() {
// 	for msg := range c.Send {
// 		err := c.Conn.WriteMessage(websocket.TextMessage, msg)
// 		if err != nil {
// 			break
// 		}
// 	}
// 	c.Conn.Close()
// }

var client = structure.Client()

func HostConnection(c *gin.Context) {

	hostID := c.Query("hostid")
	roomID := c.Query("roomid")

	if hostID == "" || roomID == "" {
		c.JSON(400, gin.H{"error": "Missing hostid or roomid"})
		return
	}

	db := client.Database("quizdb")
	collection := db.Collection("event")

	conn, err := U.Upgrade(c.Writer, c.Request, nil)
	if err != nil {
		log.Println("WebSocket Upgrade error:", err)
		return
	}

	wsClient := &HostClient{
		Conn:   conn,
		Send:   make(chan []byte, 256),
		HostID: hostID,
		RoomID: roomID,
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	var details structure.Event
	if err := collection.FindOne(ctx, bson.M{"roomid": roomID, "hostid": hostID}).Decode(&details); err != nil {
		conn.Close()
		return
	}
	hub := NewRoom(details.RoomId, details.HostID, details.Timingperquestion)
	go hub.Run()
	hub.HostRegister <- wsClient
	go wsClient.WritePump()
	HostBrodcast(hub, "Room started")

}

func (h *Room) Run() {
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
		case host := <-h.HostRegister:
			h.Hosts[host] = true

		case host := <-h.Hostunregister:
			if _, ok := h.Hosts[host]; ok {
				delete(h.Hosts, host)
				close(host.Send)
			}
		case hostMsg := <-h.HostBrodcast:
			for host := range h.Hosts {
				select {
				case host.Send <- hostMsg:
				default:
					close(host.Send)
					delete(h.Hosts, host)
				}
			}

			// case ans := <-h.Answers:
			// 	fmt.Printf("Client %s answered %s\n", ans.ClientID, ans.Choice)
		}
	}
}

// func ServeWS(hub *Hub, w http.ResponseWriter, r *http.Request, name string) {
// 	// Upgrade HTTP connection to WebSocket
// 	conn, err := U.Upgrade(w, r, nil)
// 	if err != nil {
// 		log.Println("WebSocket Upgrade error:", err)
// 		return
// 	}

// 	// Create a new client
// 	client := &Client{
// 		Conn: conn,
// 		Send: make(chan []byte, 256), // buffered channel to avoid blocking
// 		Id:   logicforadmin.GenerateRandomCode(6),
// 		Name: name, // generate unique 6-char ID
// 	}

// 	// Register the client in the hub
// 	hub.Register <- client

// 	// Start goroutines to handle incoming and outgoing messages
// 	// go client.WritePump()   // sends messages to the client
// 	go client.ReadPump(hub) // reads messages (answers) from the client
// }

// func BroadcastQuestion(hub *Room, question string) {
// 	hub.Broadcast <- []byte(question)
// }

func HostBrodcast(r *Room, question string) {
	r.HostBrodcast <- []byte(question)
}

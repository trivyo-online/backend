package logicforuser

import (
	"context"
	"encoding/json"
	"log"
	"net/http"
	"strings"
	"time"
	"trivyo_online/structure"

	"github.com/gin-gonic/gin"
	"github.com/gorilla/websocket"
	"go.mongodb.org/mongo-driver/bson"
)

var U = websocket.Upgrader{
	CheckOrigin: func(r *http.Request) bool { return true },
}

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
	_, err = collection.UpdateOne(
		ctx,
		bson.M{"roomid": roomID, "hostid": hostID},
		bson.M{"$set": bson.M{"quizstarted": true}},
	)
	if err != nil {
		log.Println("mongo update quizstarted error:", err)
	}

	var details structure.Event
	if err := collection.FindOne(ctx, bson.M{"roomid": roomID, "hostid": hostID}).Decode(&details); err != nil {
		conn.Close()
		return
	}

	rdb := structure.RedisClient()

	b, err := json.Marshal(details)
	if err != nil {
		log.Println("marshal error:", err)
		return
	}

	cmd := rdb.Do(ctx, "JSON.SET", "event:"+roomID, "$", string(b))
	if cmd.Err() != nil {
		log.Println("redis json.set error:", cmd.Err())
	}
	// set TTL for the whole key

	rdb.Expire(ctx, "event:"+roomID, 3*time.Hour)

	hub := NewRoom(details.RoomId, details.HostID, details.Timingperquestion)
	SaveRoom(hub)
	go hub.Run()
	hub.HostRegister <- wsClient

	go wsClient.HReadPump(hub)
	go wsClient.HWritePump()
}

func ConnecttoRoom(c *gin.Context) {
	roomID := c.Query("roomid")
	hostID := c.Query("hostid")
	userID := c.Query("userid")

	if roomID == "" || hostID == "" || userID == "" {
		c.JSON(400, gin.H{"error": "missing roomid/hostid/userid"})
		return
	}

	room := GetRoom(roomID)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	rdb := structure.RedisClient()

	host, err := rdb.Do(ctx, "JSON.GET", "event:"+roomID, "$.HostID").Text()
	log.Println(host)
	if err != nil {
		c.JSON(500, gin.H{"error": "error reading redis"})
		return
	}
	host = strings.Trim(host, `[]"`)
	log.Println(host)

	if host != hostID {
		log.Println("host pronlem")
		return
	}

	started, err := rdb.Do(ctx, "JSON.GET", "event:"+roomID, "$.QuizStarted").Text()
	log.Println(started)
	if err != nil {
		c.JSON(500, gin.H{"error": "cannot read QuizStarted"})
		return
	}
	started = strings.Trim(started, `"[]`)
	log.Println(started)

	if started != "true" {
		c.JSON(400, gin.H{"status": "Room is not started"})
		return
	}

	conn, err := U.Upgrade(c.Writer, c.Request, nil)
	if err != nil {
		log.Println("WebSocket Upgrade error:", err)
		return
	}

	wsClient := &UserClient{
		Userid: userID,
		Conn:   conn,
		Send:   make(chan []byte),
		RoomID: roomID,
	}

	room.Register <- wsClient

	go wsClient.UReadPump(room)
	go wsClient.UWritePump()

}

func (h *Room) Run() {
	for {
		select {

		case client := <-h.Register:
			h.Clients[client] = true
			log.Println("User registered:", client.Userid)
			client.Send <- []byte(`{"status":"connected"}`)

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

			for host := range h.Hosts {
				select {
				case host.Send <- message:
				default:
					close(host.Send)
					delete(h.Hosts, host)
				}
			}

		case host := <-h.HostRegister:
			h.Hosts[host] = true
			log.Println("Host registered:", host.HostID)

		case host := <-h.Hostunregister:
			if _, ok := h.Hosts[host]; ok {
				delete(h.Hosts, host)
				close(host.Send)
			}

		case hostMsg := <-h.HostBrodcast:
			h.QuizStarted = true

			for client := range h.Clients {
				select {
				case client.Send <- hostMsg:
				default:
					close(client.Send)
					delete(h.Clients, client)
				}
			}

			for host := range h.Hosts {
				select {
				case host.Send <- hostMsg:
				default:
					close(host.Send)
					delete(h.Hosts, host)
				}
			}
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

func UserBrodcast(r *Room, question string) {
	r.Broadcast <- []byte(question)
}

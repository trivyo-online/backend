package logicforuser

import (
	"context"
	"encoding/json"
	"log"
	"net/http"
	"regexp"
	"strings"
	"time"

	"trivyo_online/auth"
	"trivyo_online/structure"

	"github.com/gin-gonic/gin"
	"github.com/gorilla/websocket"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
)

var U = websocket.Upgrader{
	CheckOrigin: func(r *http.Request) bool { return true },
}

func events() *mongo.Collection { return structure.DB().Collection("event") }

var userRe = regexp.MustCompile(`^[\p{L}\p{N} _.-]{1,24}$`)

// HostConnection opens the host's control socket and makes the room live.
// GET /api/host?roomid=...&token=...
func HostConnection(c *gin.Context) {
	hostID, err := auth.Verify(auth.TokenFrom(c))
	if err != nil {
		c.JSON(http.StatusUnauthorized, gin.H{"error": "login required"})
		return
	}
	roomID := strings.ToUpper(c.Query("roomid"))
	if roomID == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "missing roomid"})
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	filter := bson.M{"roomid": roomID, "hostid": hostID}
	var ev structure.Event
	if err := events().FindOne(ctx, filter).Decode(&ev); err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "event not found for this host"})
		return
	}
	if _, err := events().UpdateOne(ctx, filter, bson.M{"$set": bson.M{"quizstarted": true}}); err != nil {
		log.Println("mongo update quizstarted error:", err)
	}
	ev.QuizStarted = true
	cacheEvent(ctx, ev)

	conn, err := U.Upgrade(c.Writer, c.Request, nil)
	if err != nil {
		log.Println("WebSocket Upgrade error:", err)
		return
	}

	// Reuse a running room so a host reconnect keeps its players.
	room := GetRoom(roomID)
	if room == nil {
		room = NewRoom(ev)
		SaveRoom(room)
		go room.Run()
	}

	h := &HostClient{Conn: conn, Send: make(chan []byte, sendBuffer), HostID: hostID, RoomID: roomID}
	room.hostRegister <- h
	go writePump(conn, h.Send)
	h.Send <- mustJSON(map[string]any{"type": "welcome", "role": "host", "room": room.summary(), "leaderboard": RoomBoard(roomID, 50)})
	go h.readPump(room)
}

// cacheEvent keeps a Redis copy of the live event when Redis is configured.
func cacheEvent(ctx context.Context, ev structure.Event) {
	rdb := structure.RedisClient()
	if rdb == nil {
		return
	}
	b, err := json.Marshal(ev)
	if err != nil {
		return
	}
	if err := rdb.Do(ctx, "JSON.SET", "event:"+ev.RoomId, "$", string(b)).Err(); err != nil {
		rdb.Set(ctx, "event:"+ev.RoomId, b, 3*time.Hour) // server without RedisJSON
		return
	}
	rdb.Expire(ctx, "event:"+ev.RoomId, 3*time.Hour)
}

// ConnecttoRoom joins a player to a live room.
// GET /api/connect?roomid=...&userid=...  (hostid optional; checked if given)
func ConnecttoRoom(c *gin.Context) {
	roomID := strings.ToUpper(c.Query("roomid"))
	userID := strings.TrimSpace(c.Query("userid"))
	hostID := c.Query("hostid")

	if roomID == "" || !userRe.MatchString(userID) {
		c.JSON(http.StatusBadRequest, gin.H{"error": "need roomid and a userid of 1-24 letters, digits, spaces, '.', '_' or '-'"})
		return
	}
	room := GetRoom(roomID)
	if room == nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "room is not live"})
		return
	}
	if hostID != "" && hostID != room.HostID {
		c.JSON(http.StatusBadRequest, gin.H{"error": "host mismatch"})
		return
	}
	if !room.claim(userID) {
		c.JSON(http.StatusConflict, gin.H{"error": "that name is already playing in this room"})
		return
	}

	conn, err := U.Upgrade(c.Writer, c.Request, nil)
	if err != nil {
		room.release(userID)
		log.Println("WebSocket Upgrade error:", err)
		return
	}

	u := &UserClient{Userid: userID, Conn: conn, Send: make(chan []byte, sendBuffer), RoomID: roomID}
	// Queue the welcome before registering so it is the first frame.
	u.Send <- mustJSON(map[string]any{
		"type": "welcome", "role": "player", "userid": userID, "room": room.summary(),
		"score": userScore(roomID, userID), "leaderboard": RoomBoard(roomID, 10),
	})
	if q := room.currentQuestion(); q != nil {
		u.Send <- mustJSON(q)
	}
	select {
	case room.register <- u:
	case <-room.stop:
		conn.Close()
		return
	}
	go writePump(conn, u.Send)
	go u.readPump(room)
}

type playerMsg struct {
	Type       string `json:"type"`
	QuestionNo *int   `json:"questionno"`
	Choice     *int   `json:"choice"`
	Ans        string `json:"ans"` // legacy: option text
}

func (u *UserClient) readPump(room *Room) {
	defer func() {
		select {
		case room.unregister <- u:
		case <-room.stop:
		}
		u.Conn.Close()
	}()
	prepareRead(u.Conn)
	for {
		_, raw, err := u.Conn.ReadMessage()
		if err != nil {
			return
		}
		var m playerMsg
		if err := json.Unmarshal(raw, &m); err != nil {
			room.SendTo(u, map[string]any{"type": "error", "message": "invalid JSON"})
			continue
		}
		if m.Type == "" && (m.Ans != "" || m.Choice != nil) {
			m.Type = "answer"
		}
		switch m.Type {
		case "answer":
			handleAnswer(room, u, m)
		case "ping":
			room.SendTo(u, map[string]any{"type": "pong"})
		default:
			room.SendTo(u, map[string]any{"type": "error", "message": "unknown message type"})
		}
	}
}

func handleAnswer(room *Room, u *UserClient, m playerMsg) {
	now := time.Now()
	room.mu.RLock()
	cur, opened, deadline := room.current, room.opened, room.deadline
	var q structure.Question
	if cur >= 0 && cur < len(room.questions) {
		q = room.questions[cur]
	}
	room.mu.RUnlock()

	reject := func(msg string) {
		room.SendTo(u, map[string]any{"type": "error", "message": msg})
	}
	if cur < 0 || q.Question == "" {
		reject("no question is open")
		return
	}
	if m.QuestionNo != nil && *m.QuestionNo != cur {
		reject("that question is closed")
		return
	}
	if now.After(deadline.Add(answerGrace)) {
		reject("time is up")
		return
	}
	choice := -1
	if m.Choice != nil {
		choice = *m.Choice
	} else {
		for i, o := range q.Options {
			if strings.EqualFold(strings.TrimSpace(o), strings.TrimSpace(m.Ans)) {
				choice = i
				break
			}
		}
	}
	if choice < 0 || choice >= len(q.Options) {
		reject("invalid option")
		return
	}

	correct := q.Correctoption == itoa(choice)
	points := 0
	if correct {
		points = PointsPerCorrect
	}
	a := structure.Answer{
		RoomID: room.ID, UserID: u.Userid, QuestionNo: cur, Choice: choice,
		Correct: correct, Points: points, At: now, Ms: now.Sub(opened).Milliseconds(),
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, err := structure.DB().Collection("answers").InsertOne(ctx, a); err != nil {
		if mongo.IsDuplicateKeyError(err) {
			reject("you already answered this question")
		} else {
			log.Println("answer insert:", err)
			reject("could not record answer")
		}
		return
	}

	score := userScore(room.ID, u.Userid)
	room.SendTo(u, map[string]any{"type": "result", "questionno": cur, "choice": choice, "correct": correct, "points": points, "score": score})
	room.ToHosts(map[string]any{"type": "answer", "userid": u.Userid, "questionno": cur, "choice": choice, "correct": correct, "score": score, "ms": a.Ms})
	room.Broadcast(map[string]any{"type": "leaderboard", "entries": RoomBoard(room.ID, 10)})
}

type hostMsg struct {
	Type       string `json:"type"`
	QuestionNo *int   `json:"questionno"`
}

func (h *HostClient) readPump(room *Room) {
	defer func() {
		select {
		case room.hostUnregister <- h:
		case <-room.stop:
		}
		h.Conn.Close()
	}()
	prepareRead(h.Conn)
	reply := func(v any) {
		select {
		case room.toHosts <- mustJSON(v):
		case <-room.stop:
		}
	}
	for {
		_, raw, err := h.Conn.ReadMessage()
		if err != nil {
			return
		}
		var m hostMsg
		if err := json.Unmarshal(raw, &m); err != nil {
			reply(map[string]any{"type": "error", "message": "invalid JSON"})
			continue
		}
		switch m.Type {
		case "question", "next":
			no := 0
			if m.QuestionNo != nil {
				no = *m.QuestionNo
			} else {
				room.mu.RLock()
				no = room.current + 1
				room.mu.RUnlock()
			}
			payload, ok := room.open(no)
			if !ok {
				reply(map[string]any{"type": "error", "message": "no such question"})
				continue
			}
			room.Broadcast(payload)
		case "reveal":
			room.mu.RLock()
			cur := room.current
			var correct string
			if cur >= 0 && cur < len(room.questions) {
				correct = room.questions[cur].Correctoption
			}
			room.mu.RUnlock()
			if cur < 0 {
				reply(map[string]any{"type": "error", "message": "no question to reveal"})
				continue
			}
			room.mu.Lock()
			if time.Now().Before(room.deadline) {
				room.deadline = time.Now() // close answers once revealed
			}
			room.mu.Unlock()
			room.Broadcast(map[string]any{"type": "reveal", "questionno": cur, "correctoption": atoi(correct), "stats": QuestionStats(room.ID, cur)})
		case "leaderboard":
			reply(map[string]any{"type": "leaderboard", "entries": RoomBoard(room.ID, 50)})
		case "end":
			endRoom(room)
			return
		default:
			reply(map[string]any{"type": "error", "message": "unknown message type"})
		}
	}
}

func endRoom(room *Room) {
	board := RoomBoard(room.ID, 100)
	room.Broadcast(map[string]any{"type": "end", "leaderboard": board})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, err := events().UpdateOne(ctx, bson.M{"roomid": room.ID}, bson.M{"$set": bson.M{"quizstarted": false}}); err != nil {
		log.Println("mongo end room:", err)
	}
	DropRoom(room.ID)
	// Let Run flush the final broadcast before closing every socket.
	time.AfterFunc(500*time.Millisecond, room.Stop)
}

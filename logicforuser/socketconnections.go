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
	// permessage-deflate: smaller frames on slow mobile links.
	EnableCompression: true,
	ReadBufferSize:    2048,
	WriteBufferSize:   4096,
}

func events() *mongo.Collection { return structure.DB().Collection("event") }

var userRe = regexp.MustCompile(`^[\p{L}\p{N} _.-]{1,24}$`)

// GoLive marks an event started, caches it and returns its running room.
func GoLive(ev structure.Event) *Room {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, err := events().UpdateOne(ctx, bson.M{"roomid": ev.RoomId}, bson.M{"$set": bson.M{"quizstarted": true}}); err != nil {
		log.Println("mongo update quizstarted error:", err)
	}
	ev.QuizStarted = true
	cacheEvent(ctx, ev)
	room, _ := OpenRoom(ev)
	return room
}

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
	var ev structure.Event
	if err := events().FindOne(ctx, bson.M{"roomid": roomID, "hostid": hostID}).Decode(&ev); err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "event not found for this host"})
		return
	}

	conn, err := U.Upgrade(c.Writer, c.Request, nil)
	if err != nil {
		log.Println("WebSocket Upgrade error:", err)
		return
	}
	room := GoLive(ev) // reuses a running room, so a host reconnect keeps its players

	h := &HostClient{Conn: conn, Send: make(chan []byte, sendBuffer), HostID: hostID, RoomID: roomID}
	h.Send <- mustJSON(map[string]any{"type": "welcome", "role": "host", "room": room.summary(), "leaderboard": RoomBoard(roomID, 100), "state": room.hostState()})
	select {
	case room.hostRegister <- h:
	case <-room.stop:
		conn.Close()
		return
	}
	go writePump(conn, h.Send)
	go h.readPump(room)
}

// hostState lets a reconnecting host resume the question in progress.
func (r *Room) hostState() map[string]any {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if r.current < 0 || r.current >= len(r.questions) {
		return nil
	}
	return map[string]any{
		"questionno": r.current, "deadline": r.deadline.UnixMilli(), "now": time.Now().UnixMilli(), "revealed": r.revealed,
		"secondsleft": max(0, time.Until(r.deadline).Seconds()),
		"stats":       QuestionStats(r.ID, r.current),
	}
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

// ConnecttoRoom joins (or rejoins) a player to a live room.
// GET /api/connect?roomid=...&userid=...&key=...  (hostid optional; checked if given)
func ConnecttoRoom(c *gin.Context) {
	roomID := strings.ToUpper(c.Query("roomid"))
	userID := strings.TrimSpace(c.Query("userid"))
	hostID := c.Query("hostid")
	key := c.Query("key") // private per-browser rejoin key

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
	if !room.claim(userID, key) {
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
	// Queue the resume state before registering so it arrives first:
	// welcome (score, board), then the open question and any answer
	// already given to it, so a reconnect lands exactly where it left off.
	q, cur := room.currentQuestion()
	welcome := map[string]any{
		"type": "welcome", "role": "player", "userid": userID, "room": room.summary(),
		"score": userScore(roomID, userID), "leaderboard": RoomBoard(roomID, 10),
	}
	u.Send <- mustJSON(welcome)
	if q != nil {
		u.Send <- mustJSON(q)
		if a := priorAnswer(roomID, userID, cur); a != nil {
			u.Send <- mustJSON(map[string]any{"type": "result", "questionno": cur, "choice": a.Choice, "correct": a.Correct, "points": a.Points, "score": welcome["score"], "resumed": true})
		}
	}
	select {
	case room.register <- u:
	case <-room.stop:
		room.release(userID)
		conn.Close()
		return
	}
	go writePump(conn, u.Send)
	go u.readPump(room)
}

func priorAnswer(roomID, userID string, no int) *structure.Answer {
	ctx, cancel := ctx5()
	defer cancel()
	var a structure.Answer
	if err := answers().FindOne(ctx, bson.M{"roomid": roomID, "questionno": no, "userid": userID}).Decode(&a); err != nil {
		return nil
	}
	return &a
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
	var limit bucket
	strikes := 0
	for {
		_, raw, err := u.Conn.ReadMessage()
		if err != nil {
			return
		}
		if !limit.allow(4, 8) {
			if strikes++; strikes > 20 {
				return // persistent flooding: drop the connection
			}
			room.SendTo(u, map[string]any{"type": "error", "message": "slow down"})
			continue
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
			room.SendTo(u, map[string]any{"type": "pong", "now": time.Now().UnixMilli()})
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

	reject := func(code, msg string) {
		room.SendTo(u, map[string]any{"type": "error", "code": code, "message": msg, "questionno": cur})
	}
	if cur < 0 || q.Question == "" {
		reject("no_question", "no question is open")
		return
	}
	if m.QuestionNo != nil && *m.QuestionNo != cur {
		reject("closed", "that question is closed")
		return
	}
	if now.After(deadline.Add(answerGrace)) {
		reject("late", "time is up")
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
		reject("invalid", "invalid option")
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
	if _, err := answers().InsertOne(ctx, a); err != nil {
		if mongo.IsDuplicateKeyError(err) {
			// A retry after a dropped connection: re-send the original result
			// so the client converges instead of showing an error.
			if prev := priorAnswer(room.ID, u.Userid, cur); prev != nil {
				room.SendTo(u, map[string]any{"type": "result", "questionno": cur, "choice": prev.Choice, "correct": prev.Correct, "points": prev.Points, "score": userScore(room.ID, u.Userid), "resumed": true})
				return
			}
			reject("duplicate", "you already answered this question")
		} else {
			log.Println("answer insert:", err)
			reject("server", "could not record answer")
		}
		return
	}

	score := userScore(room.ID, u.Userid)
	room.SendTo(u, map[string]any{"type": "result", "questionno": cur, "choice": choice, "correct": correct, "points": points, "score": score})
	room.ToHosts(map[string]any{"type": "answer", "userid": u.Userid, "questionno": cur, "choice": choice, "correct": correct, "score": score, "ms": a.Ms})
	room.boardDirty.Store(true)
}

type hostMsg struct {
	Type       string `json:"type"`
	QuestionNo *int   `json:"questionno"`
	Delay      int    `json:"delay"` // seconds before auto-run starts
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
			if !room.StartQuestion(no) {
				reply(map[string]any{"type": "error", "message": "no such question"})
			}
		case "reveal":
			if !room.Reveal() {
				reply(map[string]any{"type": "error", "message": "no question to reveal"})
			}
		case "auto":
			room.StartAuto(time.Now().Add(time.Duration(max(0, min(m.Delay, 3600))) * time.Second))
		case "manual":
			room.StopAuto()
		case "leaderboard":
			reply(map[string]any{"type": "hostboard", "entries": RoomBoard(room.ID, 100)})
		case "end":
			endRoom(room)
			return
		default:
			reply(map[string]any{"type": "error", "message": "unknown message type"})
		}
	}
}

func endRoom(room *Room) {
	if !dropRoom(room) {
		return // already ended (host and autopilot can race here)
	}
	room.setAuto(nil)
	board := RoomBoard(room.ID, 100)
	room.Broadcast(map[string]any{"type": "end", "leaderboard": board})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, err := events().UpdateOne(ctx, bson.M{"roomid": room.ID}, bson.M{"$set": bson.M{"quizstarted": false, "finished": true}}); err != nil {
		log.Println("mongo end room:", err)
	}
	// Let Run flush the final broadcast before closing every socket.
	time.AfterFunc(1500*time.Millisecond, room.Stop)
}

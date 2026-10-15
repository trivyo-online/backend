package logicforuser

import (
	"encoding/json"
	"log"
	"strings"
	"sync"
	"time"

	"trivyo_online/structure"

	"github.com/gorilla/websocket"
)

const (
	writeWait  = 10 * time.Second
	pongWait   = 60 * time.Second
	pingPeriod = 50 * time.Second
	sendBuffer = 64
	// Answers arriving just after the deadline still count, to absorb latency.
	answerGrace = 1500 * time.Millisecond
	// DefaultQuestionSeconds applies when a question has no Eachquestiontime.
	DefaultQuestionSeconds = 20
	PointsPerCorrect       = 50
)

type UserClient struct {
	Userid string
	Conn   *websocket.Conn
	Send   chan []byte
	RoomID string
}

type HostClient struct {
	Conn   *websocket.Conn
	Send   chan []byte
	HostID string
	RoomID string
}

type direct struct {
	to  *UserClient
	msg []byte
}

// Room is one live quiz. The Run goroutine owns the client maps; all
// sends go through its channels, so a closed Send is never written to.
type Room struct {
	ID     string
	HostID string
	Name   string

	mu        sync.RWMutex
	questions []structure.Question
	current   int // -1 until the host sends the first question
	opened    time.Time
	deadline  time.Time

	clients map[*UserClient]bool
	hosts   map[*HostClient]bool
	names   map[string]bool // connected user IDs, guarded by namesMu
	namesMu sync.Mutex

	register       chan *UserClient
	unregister     chan *UserClient
	hostRegister   chan *HostClient
	hostUnregister chan *HostClient
	broadcast      chan []byte // everyone
	toHosts        chan []byte
	direct         chan direct
	stop           chan struct{}
	stopOnce       sync.Once
}

func NewRoom(ev structure.Event) *Room {
	return &Room{
		ID:             ev.RoomId,
		HostID:         ev.HostID,
		Name:           ev.Nameofroom,
		questions:      ev.Questions,
		current:        -1,
		clients:        make(map[*UserClient]bool),
		hosts:          make(map[*HostClient]bool),
		names:          make(map[string]bool),
		register:       make(chan *UserClient, 64),
		unregister:     make(chan *UserClient, 64),
		hostRegister:   make(chan *HostClient, 8),
		hostUnregister: make(chan *HostClient, 8),
		broadcast:      make(chan []byte, 256),
		toHosts:        make(chan []byte, 256),
		direct:         make(chan direct, 256),
		stop:           make(chan struct{}),
	}
}

func (r *Room) Run() {
	deliver := func(send chan []byte, msg []byte) bool {
		select {
		case send <- msg:
			return true
		default: // client too slow; drop it
			return false
		}
	}
	for {
		select {
		case c := <-r.register:
			r.clients[c] = true
			r.toHostsNow(r.playersMsg())
		case c := <-r.unregister:
			if r.clients[c] {
				delete(r.clients, c)
				close(c.Send)
				r.release(c.Userid)
				r.toHostsNow(r.playersMsg())
			}
		case h := <-r.hostRegister:
			r.hosts[h] = true
		case h := <-r.hostUnregister:
			if r.hosts[h] {
				delete(r.hosts, h)
				close(h.Send)
			}
		case msg := <-r.broadcast:
			for c := range r.clients {
				if !deliver(c.Send, msg) {
					delete(r.clients, c)
					close(c.Send)
					r.release(c.Userid)
				}
			}
			r.toHostsNow(msg)
		case msg := <-r.toHosts:
			r.toHostsNow(msg)
		case d := <-r.direct:
			if r.clients[d.to] && !deliver(d.to.Send, d.msg) {
				delete(r.clients, d.to)
				close(d.to.Send)
				r.release(d.to.Userid)
			}
		case <-r.stop:
			for c := range r.clients {
				close(c.Send)
			}
			for h := range r.hosts {
				close(h.Send)
			}
			return
		}
	}
}

// toHostsNow must only be called from Run.
func (r *Room) toHostsNow(msg []byte) {
	for h := range r.hosts {
		select {
		case h.Send <- msg:
		default:
			delete(r.hosts, h)
			close(h.Send)
		}
	}
}

func (r *Room) playersMsg() []byte {
	list := make([]string, 0, len(r.clients))
	for c := range r.clients {
		list = append(list, c.Userid)
	}
	return mustJSON(map[string]any{"type": "players", "count": len(list), "players": list})
}

// claim reserves a user ID in this room; false if it is already connected.
func (r *Room) claim(user string) bool {
	r.namesMu.Lock()
	defer r.namesMu.Unlock()
	if r.names[strings.ToLower(user)] {
		return false
	}
	r.names[strings.ToLower(user)] = true
	return true
}

func (r *Room) release(user string) {
	r.namesMu.Lock()
	delete(r.names, strings.ToLower(user))
	r.namesMu.Unlock()
}

func (r *Room) Stop() {
	r.stopOnce.Do(func() { close(r.stop) })
}

func (r *Room) Broadcast(v any) {
	select {
	case r.broadcast <- mustJSON(v):
	case <-r.stop:
	}
}

func (r *Room) ToHosts(v any) {
	select {
	case r.toHosts <- mustJSON(v):
	case <-r.stop:
	}
}

func (r *Room) SendTo(c *UserClient, v any) {
	select {
	case r.direct <- direct{to: c, msg: mustJSON(v)}:
	case <-r.stop:
	}
}

// publicQuestion is what players see: no correct option.
func publicQuestion(q structure.Question, no, total int, deadline time.Time) map[string]any {
	secs := DefaultQuestionSeconds
	if q.Eachquestiontime != nil {
		secs = *q.Eachquestiontime
	}
	return map[string]any{
		"type":       "question",
		"questionno": no,
		"total":      total,
		"question":   map[string]any{"Question": q.Question, "Options": q.Options, "Eachquestiontime": secs},
		"deadline":   deadline.UnixMilli(),
	}
}

// open starts question no and returns its player payload.
func (r *Room) open(no int) (map[string]any, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if no < 0 || no >= len(r.questions) {
		return nil, false
	}
	q := r.questions[no]
	secs := DefaultQuestionSeconds
	if q.Eachquestiontime != nil {
		secs = *q.Eachquestiontime
	}
	r.current = no
	r.opened = time.Now()
	r.deadline = r.opened.Add(time.Duration(secs) * time.Second)
	return publicQuestion(q, no, len(r.questions), r.deadline), true
}

// currentQuestion returns the open question payload, for late joiners.
func (r *Room) currentQuestion() map[string]any {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if r.current < 0 || r.current >= len(r.questions) || time.Now().After(r.deadline) {
		return nil
	}
	return publicQuestion(r.questions[r.current], r.current, len(r.questions), r.deadline)
}

func (r *Room) summary() map[string]any {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return map[string]any{"roomid": r.ID, "name": r.Name, "hostid": r.HostID, "questioncount": len(r.questions), "current": r.current}
}

func mustJSON(v any) []byte {
	b, err := json.Marshal(v)
	if err != nil {
		log.Println("marshal:", err)
		return []byte(`{"type":"error","message":"internal error"}`)
	}
	return b
}

// writePump drains send to the socket and keeps it alive with pings.
func writePump(conn *websocket.Conn, send <-chan []byte) {
	ticker := time.NewTicker(pingPeriod)
	defer func() {
		ticker.Stop()
		conn.Close()
	}()
	for {
		select {
		case msg, ok := <-send:
			conn.SetWriteDeadline(time.Now().Add(writeWait))
			if !ok {
				conn.WriteMessage(websocket.CloseMessage, websocket.FormatCloseMessage(websocket.CloseNormalClosure, "room closed"))
				return
			}
			if err := conn.WriteMessage(websocket.TextMessage, msg); err != nil {
				return
			}
		case <-ticker.C:
			conn.SetWriteDeadline(time.Now().Add(writeWait))
			if err := conn.WriteMessage(websocket.PingMessage, nil); err != nil {
				return
			}
		}
	}
}

func prepareRead(conn *websocket.Conn) {
	conn.SetReadLimit(8 << 10)
	conn.SetReadDeadline(time.Now().Add(pongWait))
	conn.SetPongHandler(func(string) error {
		conn.SetReadDeadline(time.Now().Add(pongWait))
		return nil
	})
}

var (
	Rooms     = make(map[string]*Room)
	roomMutex sync.RWMutex
)

func SaveRoom(r *Room) {
	roomMutex.Lock()
	defer roomMutex.Unlock()
	Rooms[r.ID] = r
}

func GetRoom(id string) *Room {
	roomMutex.RLock()
	defer roomMutex.RUnlock()
	return Rooms[strings.ToUpper(id)]
}

func DropRoom(id string) {
	roomMutex.Lock()
	defer roomMutex.Unlock()
	delete(Rooms, id)
}

func IsLive(id string) bool {
	return GetRoom(id) != nil
}

// RefreshQuestions updates a live room after the editor saves.
func RefreshQuestions(id string, qs []structure.Question) {
	r := GetRoom(id)
	if r == nil {
		return
	}
	r.mu.Lock()
	r.questions = qs
	r.mu.Unlock()
	r.ToHosts(map[string]any{"type": "room", "room": r.summary()})
}

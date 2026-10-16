package logicforuser

import (
	"context"
	"encoding/json"
	"log"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"trivyo_online/structure"

	"github.com/gorilla/websocket"
)

const (
	writeWait  = 10 * time.Second
	pongWait   = 60 * time.Second
	pingPeriod = 25 * time.Second // short enough to keep mobile NATs open
	sendBuffer = 64
	closeGrace = time.Second
	// Answers arriving just after the deadline still count, to absorb latency.
	answerGrace = 1500 * time.Millisecond
	// DefaultQuestionSeconds applies when a question has no Eachquestiontime.
	DefaultQuestionSeconds = 20
	PointsPerCorrect       = 50
	// Leaderboard pushes are coalesced to at most one per boardEvery.
	boardEvery = 1 * time.Second
	// Autopilot pause on the revealed answer before the next question.
	revealPause = 4 * time.Second
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

type nameClaim struct {
	key  string
	refs int // live connections using the name, plus any mid-handshake
}

// Room is one live quiz. The Run goroutine owns the client maps; all
// sends go through its channels, so a closed Send is never written to.
type Room struct {
	ID     string
	HostID string
	Name   string

	mu         sync.RWMutex
	questions  []structure.Question
	current    int // -1 until the first question opens
	opened     time.Time
	deadline   time.Time
	revealed   bool
	startsAt   time.Time // scheduled start, zero if manual
	autoCancel context.CancelFunc

	boardDirty atomic.Bool

	clients map[*UserClient]bool
	hosts   map[*HostClient]bool
	names   map[string]*nameClaim // lowercased user ID → claim, guarded by namesMu
	namesMu sync.Mutex

	register       chan *UserClient
	unregister     chan *UserClient
	hostRegister   chan *HostClient
	hostUnregister chan *HostClient
	broadcast      chan []byte // everyone
	toHosts        chan []byte
	direct         chan direct
	resetPlayers   chan chan int // Run kicks every player and replies with the count
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
		names:          make(map[string]*nameClaim),
		register:       make(chan *UserClient, 256),
		unregister:     make(chan *UserClient, 256),
		hostRegister:   make(chan *HostClient, 8),
		hostUnregister: make(chan *HostClient, 8),
		broadcast:      make(chan []byte, 256),
		toHosts:        make(chan []byte, 1024),
		direct:         make(chan direct, 1024),
		resetPlayers:   make(chan chan int),
		stop:           make(chan struct{}),
	}
}

func (r *Room) Run() {
	deliver := func(send chan []byte, msg []byte) bool {
		select {
		case send <- msg:
			return true
		default: // client too slow; drop it and let it reconnect
			return false
		}
	}
	drop := func(c *UserClient) {
		delete(r.clients, c)
		close(c.Send)
		r.release(c.Userid)
	}
	playersChanged := false
	tick := time.NewTicker(time.Second)
	defer tick.Stop()
	for {
		select {
		case c := <-r.register:
			// A rejoin with the same name replaces the stale connection.
			for old := range r.clients {
				if strings.EqualFold(old.Userid, c.Userid) {
					drop(old)
				}
			}
			r.clients[c] = true
			playersChanged = true
		case c := <-r.unregister:
			if r.clients[c] {
				drop(c)
				playersChanged = true
			}
		case h := <-r.hostRegister:
			r.hosts[h] = true
			deliver(h.Send, r.playersMsg())
		case h := <-r.hostUnregister:
			if r.hosts[h] {
				delete(r.hosts, h)
				close(h.Send)
			}
		case msg := <-r.broadcast:
			for c := range r.clients {
				if !deliver(c.Send, msg) {
					drop(c)
					playersChanged = true
				}
			}
			r.toHostsNow(msg)
		case msg := <-r.toHosts:
			r.toHostsNow(msg)
		case d := <-r.direct:
			if r.clients[d.to] && !deliver(d.to.Send, d.msg) {
				drop(d.to)
				playersChanged = true
			}
		case done := <-r.resetPlayers:
			msg := mustJSON(map[string]any{"type": "reset", "message": "the host reset the room; join again"})
			n := len(r.clients)
			for c := range r.clients {
				deliver(c.Send, msg) // queued ahead of the close, so it still arrives
				drop(c)
			}
			playersChanged = false
			r.toHostsNow(r.playersMsg())
			done <- n
		case <-tick.C:
			// Joins come in bursts; tell hosts once a second at most.
			if playersChanged {
				playersChanged = false
				r.toHostsNow(r.playersMsg())
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

// playersMsg must only be called from Run.
func (r *Room) playersMsg() []byte {
	list := make([]string, 0, len(r.clients))
	for c := range r.clients {
		list = append(list, c.Userid)
	}
	return mustJSON(map[string]any{"type": "players", "count": len(list), "players": list})
}

// boardLoop pushes the room leaderboard when answers have changed it,
// at most once per boardEvery, instead of once per answer.
func (r *Room) boardLoop() {
	t := time.NewTicker(boardEvery)
	defer t.Stop()
	for {
		select {
		case <-r.stop:
			return
		case <-t.C:
			if r.boardDirty.Swap(false) {
				r.Broadcast(map[string]any{"type": "leaderboard", "entries": RoomBoard(r.ID, 10)})
				r.ToHosts(map[string]any{"type": "hostboard", "entries": RoomBoard(r.ID, 100)})
			}
		}
	}
}

// claim reserves a user ID in this room. The same key may claim it again
// (a refresh or reconnect takes over the old socket); a different key is
// refused while any connection holds the name. Every successful claim must
// be matched by exactly one release.
func (r *Room) claim(user, key string) bool {
	r.namesMu.Lock()
	defer r.namesMu.Unlock()
	n := strings.ToLower(user)
	if c, ok := r.names[n]; ok {
		if c.key != key {
			return false
		}
		c.refs++
		return true
	}
	r.names[n] = &nameClaim{key: key, refs: 1}
	return true
}

func (r *Room) release(user string) {
	r.namesMu.Lock()
	defer r.namesMu.Unlock()
	n := strings.ToLower(user)
	if c, ok := r.names[n]; ok {
		if c.refs--; c.refs <= 0 {
			delete(r.names, n)
		}
	}
}

func (r *Room) Stop() {
	r.stopOnce.Do(func() {
		r.setAuto(nil)
		close(r.stop)
	})
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

func seconds(q structure.Question) int {
	if q.Eachquestiontime != nil && *q.Eachquestiontime > 0 {
		return *q.Eachquestiontime
	}
	return DefaultQuestionSeconds
}

// publicQuestion is what players see: no correct option.
func publicQuestion(q structure.Question, no, total int, deadline time.Time) map[string]any {
	return map[string]any{
		"type":       "question",
		"questionno": no,
		"total":      total,
		"question":   map[string]any{"Question": q.Question, "Options": q.Options, "Eachquestiontime": seconds(q)},
		"deadline":   deadline.UnixMilli(),
		"now":        time.Now().UnixMilli(),
	}
}

// StartQuestion opens question no and sends it to everyone.
func (r *Room) StartQuestion(no int) bool {
	r.mu.Lock()
	if no < 0 || no >= len(r.questions) {
		r.mu.Unlock()
		return false
	}
	q := r.questions[no]
	r.current = no
	r.opened = time.Now()
	r.deadline = r.opened.Add(time.Duration(seconds(q)) * time.Second)
	r.revealed = false
	payload := publicQuestion(q, no, len(r.questions), r.deadline)
	r.mu.Unlock()
	r.Broadcast(payload)
	return true
}

// Reveal closes the open question and shows the answer and distribution.
func (r *Room) Reveal() bool {
	r.mu.Lock()
	cur := r.current
	if cur < 0 || cur >= len(r.questions) {
		r.mu.Unlock()
		return false
	}
	correct := r.questions[cur].Correctoption
	if time.Now().Before(r.deadline) {
		r.deadline = time.Now()
	}
	r.revealed = true
	r.mu.Unlock()
	r.Broadcast(map[string]any{"type": "reveal", "questionno": cur, "correctoption": atoi(correct), "stats": QuestionStats(r.ID, cur)})
	return true
}

// ResetPlayers stops autopilot, puts the room back in its lobby and
// disconnects every player, so the next run starts empty. It returns how
// many players were removed. Stored answers are cleared by ResetRoom.
func (r *Room) ResetPlayers() int {
	r.setAuto(nil)
	r.mu.Lock()
	r.current = -1
	r.revealed = false
	r.opened, r.deadline, r.startsAt = time.Time{}, time.Time{}, time.Time{}
	r.mu.Unlock()
	done := make(chan int, 1)
	select {
	case r.resetPlayers <- done:
	case <-r.stop:
		return 0
	}
	select {
	case n := <-done:
		return n
	case <-r.stop:
		return 0
	}
}

// currentQuestion returns the open question payload for a (re)joining player.
func (r *Room) currentQuestion() (map[string]any, int) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if r.current < 0 || r.current >= len(r.questions) || r.revealed || time.Now().After(r.deadline) {
		return nil, -1
	}
	return publicQuestion(r.questions[r.current], r.current, len(r.questions), r.deadline), r.current
}

func (r *Room) summary() map[string]any {
	r.mu.RLock()
	defer r.mu.RUnlock()
	s := map[string]any{"roomid": r.ID, "name": r.Name, "hostid": r.HostID, "questioncount": len(r.questions), "current": r.current, "auto": r.autoCancel != nil}
	if !r.startsAt.IsZero() {
		s["startsat"] = r.startsAt.UnixMilli()
	}
	return s
}

// setAuto replaces the autopilot cancel func, stopping any previous run.
func (r *Room) setAuto(cancel context.CancelFunc) {
	r.mu.Lock()
	prev := r.autoCancel
	r.autoCancel = cancel
	r.mu.Unlock()
	if prev != nil {
		prev()
	}
}

// StartAuto runs the quiz on its own: each question for its time, a short
// reveal, then the next, then the end. at is when the first question opens.
func (r *Room) StartAuto(at time.Time) {
	ctx, cancel := context.WithCancel(context.Background())
	r.setAuto(cancel)
	r.mu.Lock()
	if at.After(time.Now()) {
		r.startsAt = at
	}
	r.mu.Unlock()
	r.ToHosts(map[string]any{"type": "room", "room": r.summary()})
	r.Broadcast(map[string]any{"type": "room", "room": r.summary()})
	go r.autopilot(ctx, at)
}

func (r *Room) StopAuto() {
	r.setAuto(nil)
	r.mu.Lock()
	r.startsAt = time.Time{}
	r.mu.Unlock()
	r.ToHosts(map[string]any{"type": "room", "room": r.summary()})
}

func (r *Room) autopilot(ctx context.Context, at time.Time) {
	sleep := func(d time.Duration) bool {
		if d <= 0 {
			return ctx.Err() == nil
		}
		t := time.NewTimer(d)
		defer t.Stop()
		select {
		case <-ctx.Done():
			return false
		case <-r.stop:
			return false
		case <-t.C:
			return true
		}
	}
	if !sleep(time.Until(at)) {
		return
	}
	r.mu.Lock()
	r.startsAt = time.Time{}
	r.mu.Unlock()
	for {
		r.mu.RLock()
		cur, total, deadline, revealed := r.current, len(r.questions), r.deadline, r.revealed
		r.mu.RUnlock()
		// Let a question the host already opened run out first.
		if cur >= 0 && !revealed {
			if !sleep(time.Until(deadline.Add(answerGrace))) {
				return
			}
			r.Reveal()
			if !sleep(revealPause) {
				return
			}
		}
		if cur+1 >= total {
			endRoom(r)
			return
		}
		r.StartQuestion(cur + 1)
	}
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
				// Give the peer a moment to read the last frames and answer the
				// close; closing the TCP socket at once can reset it and lose them.
				time.Sleep(closeGrace)
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

// bucket is a small token-bucket rate limiter for one connection.
type bucket struct {
	tokens float64
	last   time.Time
}

func (b *bucket) allow(rate, burst float64) bool {
	now := time.Now()
	if b.last.IsZero() {
		b.tokens = burst
	} else {
		b.tokens = min(burst, b.tokens+now.Sub(b.last).Seconds()*rate)
	}
	b.last = now
	if b.tokens < 1 {
		return false
	}
	b.tokens--
	return true
}

var (
	Rooms     = make(map[string]*Room)
	roomMutex sync.RWMutex
)

func GetRoom(id string) *Room {
	roomMutex.RLock()
	defer roomMutex.RUnlock()
	return Rooms[strings.ToUpper(id)]
}

// dropRoom unregisters r if it is still the live room for its ID; only
// the first caller gets true.
func dropRoom(r *Room) bool {
	roomMutex.Lock()
	defer roomMutex.Unlock()
	if Rooms[r.ID] != r {
		return false
	}
	delete(Rooms, r.ID)
	return true
}

func IsLive(id string) bool {
	return GetRoom(id) != nil
}

// OpenRoom returns the live room for ev, starting it if needed.
func OpenRoom(ev structure.Event) (*Room, bool) {
	roomMutex.Lock()
	defer roomMutex.Unlock()
	if r := Rooms[ev.RoomId]; r != nil {
		return r, false
	}
	r := NewRoom(ev)
	Rooms[ev.RoomId] = r
	go r.Run()
	go r.boardLoop()
	return r, true
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

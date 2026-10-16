package logicforuser

import (
	"encoding/json"
	"testing"
	"time"

	"trivyo_online/structure"
)

// recv reads one frame, failing the test if none arrives in time.
func recv(t *testing.T, ch <-chan []byte) (map[string]any, bool) {
	t.Helper()
	select {
	case b, ok := <-ch:
		if !ok {
			return nil, false
		}
		var m map[string]any
		if err := json.Unmarshal(b, &m); err != nil {
			t.Fatalf("bad frame %s: %v", b, err)
		}
		return m, true
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for a frame")
		return nil, false
	}
}

func TestResetPlayersKicksEveryoneAndReopensLobby(t *testing.T) {
	r := NewRoom(structure.Event{RoomId: "RESET1", Questions: []structure.Question{{Question: "q", Options: []string{"a", "b"}, Correctoption: "1"}}})
	go r.Run()
	defer r.Stop()

	h := &HostClient{Send: make(chan []byte, sendBuffer), RoomID: r.ID}
	r.hostRegister <- h
	recv(t, h.Send) // initial players frame

	var players []*UserClient
	for _, name := range []string{"ana", "ben"} {
		if !r.claim(name, name+"-key") {
			t.Fatalf("claim %s", name)
		}
		u := &UserClient{Userid: name, Send: make(chan []byte, sendBuffer), RoomID: r.ID}
		r.register <- u
		players = append(players, u)
	}
	r.StartQuestion(0)
	r.StartAuto(time.Now().Add(time.Hour))

	if n := r.ResetPlayers(); n != 2 {
		t.Fatalf("ResetPlayers removed %d players, want 2", n)
	}

	for _, u := range players {
		var got []string
		for {
			m, ok := recv(t, u.Send)
			if !ok {
				break
			}
			got = append(got, m["type"].(string))
		}
		if len(got) == 0 || got[len(got)-1] != "reset" {
			t.Errorf("%s frames %v: want the last one to be reset, then the socket closed", u.Userid, got)
		}
	}

	// The host sees the empty room straight away.
	for {
		m, _ := recv(t, h.Send)
		if m["type"] == "players" && m["count"] == float64(0) {
			break
		}
	}

	r.mu.RLock()
	cur, auto, starts := r.current, r.autoCancel, r.startsAt
	r.mu.RUnlock()
	if cur != -1 || auto != nil || !starts.IsZero() {
		t.Errorf("room not back in its lobby: current=%d auto=%v startsAt=%v", cur, auto != nil, starts)
	}
	if q, _ := r.currentQuestion(); q != nil {
		t.Error("a question is still open after reset")
	}
	// Names are free again, even with a different key.
	if !r.claim("ana", "new-key") {
		t.Error("ana cannot rejoin after reset")
	}
}

func TestResetPlayersOnStoppedRoom(t *testing.T) {
	r := NewRoom(structure.Event{RoomId: "RESET2"})
	go r.Run()
	r.Stop()
	done := make(chan int)
	go func() { done <- r.ResetPlayers() }()
	select {
	case n := <-done:
		if n != 0 {
			t.Errorf("got %d, want 0", n)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("ResetPlayers blocked on a stopped room")
	}
}

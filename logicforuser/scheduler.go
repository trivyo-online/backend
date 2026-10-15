package logicforuser

import (
	"log"
	"time"

	"trivyo_online/structure"

	"go.mongodb.org/mongo-driver/bson"
)

const (
	// LobbyLead is how early a scheduled room opens so players can join.
	LobbyLead     = 10 * time.Minute
	scheduleEvery = 15 * time.Second
	// A scheduled test this far past its start without having run is skipped.
	staleAfter = time.Hour
)

// StartScheduler opens scheduled, auto-start events as lobbies ahead of
// time and runs them on autopilot from their start time.
func StartScheduler() {
	go func() {
		t := time.NewTicker(scheduleEvery)
		defer t.Stop()
		for {
			openDue(time.Now())
			<-t.C
		}
	}()
}

func openDue(now time.Time) {
	ctx, cancel := ctx5()
	defer cancel()
	cur, err := events().Find(ctx, bson.M{
		"autostart":   true,
		"finished":    bson.M{"$ne": true},
		"scheduledat": bson.M{"$lte": now.Add(LobbyLead), "$gte": now.Add(-staleAfter)},
	})
	if err != nil {
		log.Println("scheduler:", err)
		return
	}
	var due []structure.Event
	if err := cur.All(ctx, &due); err != nil {
		log.Println("scheduler decode:", err)
		return
	}
	for _, ev := range due {
		if ev.ScheduledAt == nil || len(ev.Questions) == 0 || IsLive(ev.RoomId) {
			continue
		}
		room := GoLive(ev)
		start := *ev.ScheduledAt
		if start.Before(now) {
			start = now.Add(10 * time.Second) // missed by a tick or a restart
		}
		room.StartAuto(start)
		log.Printf("scheduler: opened %s, auto-start at %s", ev.RoomId, start.Format(time.RFC3339))
	}
}

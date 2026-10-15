package logicforuser

import (
	"context"
	"log"
	"net/http"
	"strconv"
	"strings"
	"time"

	"trivyo_online/structure"

	"github.com/gin-gonic/gin"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
)

type BoardEntry struct {
	Rank     int    `json:"rank" bson:"-"`
	UserID   string `json:"userid" bson:"_id"`
	Score    int    `json:"score" bson:"score"`
	Correct  int    `json:"correct" bson:"correct"`
	Answered int    `json:"answered" bson:"answered"`
	TotalMs  int64  `json:"totalms" bson:"totalms"`
	Rooms    int    `json:"rooms,omitempty" bson:"rooms"`
}

func answers() *mongo.Collection { return structure.DB().Collection("answers") }

func ctx5() (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.Background(), 5*time.Second)
}

func itoa(i int) string { return strconv.Itoa(i) }

func atoi(s string) int {
	i, err := strconv.Atoi(s)
	if err != nil {
		return -1
	}
	return i
}

// board ranks players by score, breaking ties by total answer time.
func board(match bson.M, limit int, withRooms bool) []BoardEntry {
	group := bson.M{
		"_id":      "$userid",
		"score":    bson.M{"$sum": "$points"},
		"correct":  bson.M{"$sum": bson.M{"$cond": bson.A{"$correct", 1, 0}}},
		"answered": bson.M{"$sum": 1},
		"totalms":  bson.M{"$sum": "$ms"},
	}
	if withRooms {
		group["roomset"] = bson.M{"$addToSet": "$roomid"}
	}
	pipe := mongo.Pipeline{
		{{Key: "$match", Value: match}},
		{{Key: "$group", Value: group}},
	}
	if withRooms {
		pipe = append(pipe, bson.D{{Key: "$addFields", Value: bson.M{"rooms": bson.M{"$size": "$roomset"}}}})
	}
	pipe = append(pipe,
		bson.D{{Key: "$sort", Value: bson.D{{Key: "score", Value: -1}, {Key: "totalms", Value: 1}, {Key: "_id", Value: 1}}}},
		bson.D{{Key: "$limit", Value: limit}},
	)
	ctx, cancel := ctx5()
	defer cancel()
	cur, err := answers().Aggregate(ctx, pipe)
	if err != nil {
		log.Println("leaderboard:", err)
		return []BoardEntry{}
	}
	out := []BoardEntry{}
	if err := cur.All(ctx, &out); err != nil {
		log.Println("leaderboard decode:", err)
		return []BoardEntry{}
	}
	for i := range out {
		out[i].Rank = i + 1
	}
	return out
}

func RoomBoard(roomID string, limit int) []BoardEntry {
	return board(bson.M{"roomid": roomID}, limit, false)
}

func userScore(roomID, userID string) int {
	b := board(bson.M{"roomid": roomID, "userid": userID}, 1, false)
	if len(b) == 0 {
		return 0
	}
	return b[0].Score
}

type QStat struct {
	QuestionNo    int      `json:"questionno"`
	Question      string   `json:"question"`
	Options       []string `json:"options"`
	Correctoption int      `json:"correctoption"`
	Counts        []int    `json:"counts"`
	Total         int      `json:"total"`
	Correct       int      `json:"correct"`
	AvgMs         int64    `json:"avgms"`
}

// roomStats returns per-question answer distributions for a room.
func roomStats(roomID string, qs []structure.Question) []QStat {
	ctx, cancel := ctx5()
	defer cancel()
	cur, err := answers().Aggregate(ctx, mongo.Pipeline{
		{{Key: "$match", Value: bson.M{"roomid": roomID}}},
		{{Key: "$group", Value: bson.M{
			"_id":   bson.M{"q": "$questionno", "c": "$choice"},
			"n":     bson.M{"$sum": 1},
			"right": bson.M{"$sum": bson.M{"$cond": bson.A{"$correct", 1, 0}}},
			"ms":    bson.M{"$sum": "$ms"},
		}}},
	})
	if err != nil {
		log.Println("stats:", err)
		return []QStat{}
	}
	var rows []struct {
		ID struct {
			Q int `bson:"q"`
			C int `bson:"c"`
		} `bson:"_id"`
		N     int   `bson:"n"`
		Right int   `bson:"right"`
		Ms    int64 `bson:"ms"`
	}
	if err := cur.All(ctx, &rows); err != nil {
		return []QStat{}
	}
	out := make([]QStat, len(qs))
	ms := make([]int64, len(qs))
	for i, q := range qs {
		out[i] = QStat{QuestionNo: i, Question: q.Question, Options: q.Options, Correctoption: atoi(q.Correctoption), Counts: make([]int, len(q.Options))}
	}
	for _, r := range rows {
		if r.ID.Q < 0 || r.ID.Q >= len(out) {
			continue // question was deleted after being answered
		}
		s := &out[r.ID.Q]
		if r.ID.C >= 0 && r.ID.C < len(s.Counts) {
			s.Counts[r.ID.C] += r.N
		}
		s.Total += r.N
		s.Correct += r.Right
		ms[r.ID.Q] += r.Ms
	}
	for i := range out {
		if out[i].Total > 0 {
			out[i].AvgMs = ms[i] / int64(out[i].Total)
		}
	}
	return out
}

// QuestionStats is the distribution for one question of a live room.
func QuestionStats(roomID string, no int) *QStat {
	r := GetRoom(roomID)
	if r == nil {
		return nil
	}
	r.mu.RLock()
	qs := append([]structure.Question(nil), r.questions...)
	r.mu.RUnlock()
	s := roomStats(roomID, qs)
	if no < 0 || no >= len(s) {
		return nil
	}
	return &s[no]
}

func limitParam(c *gin.Context, def, max int) int {
	n, err := strconv.Atoi(c.DefaultQuery("limit", strconv.Itoa(def)))
	if err != nil || n < 1 {
		return def
	}
	if n > max {
		return max
	}
	return n
}

// GET /api/rooms/:roomid — public room info, used before joining.
func RoomInfo(c *gin.Context) {
	id := strings.ToUpper(c.Param("roomid"))
	ctx, cancel := ctx5()
	defer cancel()
	var ev structure.Event
	if err := events().FindOne(ctx, bson.M{"roomid": id}).Decode(&ev); err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "room not found"})
		return
	}
	c.JSON(http.StatusOK, gin.H{
		"roomid": ev.RoomId, "name": ev.Nameofroom, "hostid": ev.HostID, "date": ev.DateofQuiz,
		"starttime": ev.StartTime, "questioncount": len(ev.Questions), "live": IsLive(id),
	})
}

// GET /api/rooms/:roomid/leaderboard
func RoomLeaderboard(c *gin.Context) {
	id := strings.ToUpper(c.Param("roomid"))
	c.JSON(http.StatusOK, gin.H{"roomid": id, "entries": RoomBoard(id, limitParam(c, 50, 500))})
}

// GET /api/rooms/:roomid/stats — per-question answer distribution.
// Revealing correct options is fine here: stats are meant for after play,
// and for live rooms only the host sees them (via the reveal message).
func RoomStats(c *gin.Context) {
	id := strings.ToUpper(c.Param("roomid"))
	if IsLive(id) {
		c.JSON(http.StatusConflict, gin.H{"error": "stats open once the room has ended"})
		return
	}
	ctx, cancel := ctx5()
	defer cancel()
	var ev structure.Event
	if err := events().FindOne(ctx, bson.M{"roomid": id}).Decode(&ev); err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "room not found"})
		return
	}
	c.JSON(http.StatusOK, gin.H{"roomid": id, "name": ev.Nameofroom, "questions": roomStats(id, ev.Questions)})
}

// GET /api/leaderboard — all-time ranking across every room.
func GlobalLeaderboard(c *gin.Context) {
	ctx, cancel := ctx5()
	defer cancel()
	players, _ := answers().Distinct(ctx, "userid", bson.M{})
	rooms, _ := answers().Distinct(ctx, "roomid", bson.M{})
	total, _ := answers().CountDocuments(ctx, bson.M{})
	right, _ := answers().CountDocuments(ctx, bson.M{"correct": true})
	c.JSON(http.StatusOK, gin.H{
		"entries": board(bson.M{}, limitParam(c, 50, 500), true),
		"totals":  gin.H{"players": len(players), "rooms": len(rooms), "answers": total, "correct": right},
	})
}

// GET /api/rooms — recently played rooms, for the rankings page.
func RecentRooms(c *gin.Context) {
	ctx, cancel := ctx5()
	defer cancel()
	cur, err := answers().Aggregate(ctx, mongo.Pipeline{
		{{Key: "$group", Value: bson.M{"_id": "$roomid", "players": bson.M{"$addToSet": "$userid"}, "last": bson.M{"$max": "$at"}}}},
		{{Key: "$sort", Value: bson.M{"last": -1}}},
		{{Key: "$limit", Value: limitParam(c, 20, 100)}},
		{{Key: "$lookup", Value: bson.M{"from": "event", "localField": "_id", "foreignField": "roomid", "as": "ev"}}},
	})
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "could not list rooms"})
		return
	}
	var rows []struct {
		ID      string    `bson:"_id"`
		Players []string  `bson:"players"`
		Last    time.Time `bson:"last"`
		Ev      []struct {
			Name string `bson:"nameofroom"`
		} `bson:"ev"`
	}
	if err := cur.All(ctx, &rows); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "could not list rooms"})
		return
	}
	out := make([]gin.H, 0, len(rows))
	for _, r := range rows {
		name := ""
		if len(r.Ev) > 0 {
			name = r.Ev[0].Name
		}
		out = append(out, gin.H{"roomid": r.ID, "name": name, "players": len(r.Players), "last": r.Last, "live": IsLive(r.ID)})
	}
	c.JSON(http.StatusOK, gin.H{"rooms": out})
}

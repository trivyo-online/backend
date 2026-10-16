package logicforadmin

import (
	"context"
	"crypto/rand"
	"math/big"
	"net/http"
	"strconv"
	"strings"
	"time"

	"trivyo_online/auth"
	"trivyo_online/logicforuser"
	"trivyo_online/structure"

	"github.com/gin-gonic/gin"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

func GenerateRandomCode(n int) string {
	const letters = "ABCDEFGHJKLMNPQRSTUVWXYZ23456789" // no 0/O or 1/I lookalikes
	code := make([]byte, n)
	for i := range code {
		x, _ := rand.Int(rand.Reader, big.NewInt(int64(len(letters))))
		code[i] = letters[x.Int64()]
	}
	return string(code)
}

func events() *mongo.Collection {
	return structure.DB().Collection("event")
}

func timeout() (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.Background(), 5*time.Second)
}

// validate trims a question and checks it is answerable.
func validate(q *structure.Question) string {
	q.Question = strings.TrimSpace(q.Question)
	if q.Question == "" {
		return "question text is required"
	}
	opts := q.Options[:0]
	for _, o := range q.Options {
		if o = strings.TrimSpace(o); o != "" {
			opts = append(opts, o)
		}
	}
	q.Options = opts
	if len(q.Options) < 2 || len(q.Options) > 6 {
		return "each question needs 2-6 options"
	}
	i, err := strconv.Atoi(q.Correctoption)
	if err != nil || i < 0 || i >= len(q.Options) {
		return "correctoption must be the index of one of the options"
	}
	if q.Eachquestiontime != nil && (*q.Eachquestiontime < 5 || *q.Eachquestiontime > 300) {
		return "eachquestiontime must be between 5 and 300 seconds"
	}
	return ""
}

func validateAll(qs []structure.Question) string {
	for i := range qs {
		if msg := validate(&qs[i]); msg != "" {
			return "question " + strconv.Itoa(i+1) + ": " + msg
		}
	}
	return ""
}

// owned loads a room that belongs to the logged-in admin, writing the
// error response itself when it does not.
func owned(c *gin.Context, roomID string) (*structure.Event, bool) {
	ctx, cancel := timeout()
	defer cancel()
	var ev structure.Event
	err := events().FindOne(ctx, bson.M{"roomid": strings.ToUpper(roomID), "hostid": auth.Current(c)}).Decode(&ev)
	if err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "event not found"})
		return nil, false
	}
	if ev.Questions == nil {
		ev.Questions = []structure.Question{}
	}
	return &ev, true
}

type eventInput struct {
	Nameofroom  string
	StartTime   string
	DateofQuiz  string
	Questions   []structure.Question
	ScheduledAt *time.Time // RFC 3339; null clears the schedule
	AutoStart   bool
}

func (in *eventInput) check() string {
	in.Nameofroom = strings.TrimSpace(in.Nameofroom)
	if in.Nameofroom == "" {
		return "event name is required"
	}
	if in.Questions == nil {
		in.Questions = []structure.Question{}
	}
	if in.AutoStart && in.ScheduledAt == nil {
		return "auto-start needs a scheduled time"
	}
	return validateAll(in.Questions)
}

func InsertEvent(c *gin.Context) {
	var in eventInput
	if err := c.BindJSON(&in); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid JSON"})
		return
	}
	if msg := in.check(); msg != "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": msg})
		return
	}

	ev := structure.Event{
		ScheduledAt: in.ScheduledAt,
		AutoStart:   in.AutoStart,
		Nameofroom:  in.Nameofroom,
		StartTime:   in.StartTime,
		DateofQuiz:  in.DateofQuiz,
		HostID:      auth.Current(c),
		Questions:   in.Questions,
		CreatedAt:   time.Now(),
	}
	ctx, cancel := timeout()
	defer cancel()
	// Room codes are random; retry on the rare collision with the unique index.
	for attempt := 0; attempt < 5; attempt++ {
		ev.RoomId = GenerateRandomCode(8)
		res, err := events().InsertOne(ctx, ev)
		if mongo.IsDuplicateKeyError(err) {
			continue
		}
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to insert"})
			return
		}
		c.JSON(http.StatusOK, gin.H{"message": "Event inserted", "id": res.InsertedID, "roomid": ev.RoomId, "event": ev})
		return
	}
	c.JSON(http.StatusInternalServerError, gin.H{"error": "could not allocate a room code"})
}

func AddQuestionHandler(c *gin.Context) {
	var req struct {
		RoomId   string             `json:"roomid"`
		Question structure.Question `json:"question"`
	}
	if err := c.BindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid JSON"})
		return
	}
	if msg := validate(&req.Question); msg != "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": msg})
		return
	}
	ev, ok := owned(c, req.RoomId)
	if !ok {
		return
	}
	ctx, cancel := timeout()
	defer cancel()
	if _, err := events().UpdateOne(ctx, bson.M{"roomid": ev.RoomId}, bson.M{"$push": bson.M{"questions": req.Question}}); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to add question"})
		return
	}
	logicforuser.RefreshQuestions(ev.RoomId, append(ev.Questions, req.Question))
	c.JSON(http.StatusOK, gin.H{"message": "Question added", "count": len(ev.Questions) + 1})
}

func ListEvents(c *gin.Context) {
	ctx, cancel := timeout()
	defer cancel()
	cur, err := events().Find(ctx, bson.M{"hostid": auth.Current(c)}, options.Find().SetSort(bson.D{{Key: "createdat", Value: -1}}))
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "could not list events"})
		return
	}
	list := []structure.Event{}
	if err := cur.All(ctx, &list); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "could not list events"})
		return
	}
	out := make([]gin.H, 0, len(list))
	for _, ev := range list {
		out = append(out, gin.H{
			"RoomId": ev.RoomId, "Nameofroom": ev.Nameofroom, "DateofQuiz": ev.DateofQuiz, "StartTime": ev.StartTime,
			"QuestionCount": len(ev.Questions), "Live": logicforuser.IsLive(ev.RoomId), "CreatedAt": ev.CreatedAt,
			"ScheduledAt": ev.ScheduledAt, "AutoStart": ev.AutoStart, "Finished": ev.Finished,
		})
	}
	c.JSON(http.StatusOK, gin.H{"events": out})
}

func GetEvent(c *gin.Context) {
	if ev, ok := owned(c, c.Param("roomid")); ok {
		c.JSON(http.StatusOK, gin.H{"event": ev, "live": logicforuser.IsLive(ev.RoomId)})
	}
}

// UpdateEvent replaces the event's details and full question list; the
// editor saves through this.
func UpdateEvent(c *gin.Context) {
	ev, ok := owned(c, c.Param("roomid"))
	if !ok {
		return
	}
	var in eventInput
	if err := c.BindJSON(&in); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid JSON"})
		return
	}
	if msg := in.check(); msg != "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": msg})
		return
	}
	set := bson.M{
		"nameofroom": in.Nameofroom, "starttime": in.StartTime, "dateofquiz": in.DateofQuiz, "questions": in.Questions,
		"autostart": in.AutoStart, "scheduledat": in.ScheduledAt,
	}
	// A new schedule makes a finished test runnable again.
	if in.ScheduledAt != nil && (ev.ScheduledAt == nil || !in.ScheduledAt.Equal(*ev.ScheduledAt)) {
		set["finished"] = false
	}
	ctx, cancel := timeout()
	defer cancel()
	_, err := events().UpdateOne(ctx, bson.M{"roomid": ev.RoomId}, bson.M{"$set": set})
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "could not save event"})
		return
	}
	logicforuser.RefreshQuestions(ev.RoomId, in.Questions)
	ev.Nameofroom, ev.StartTime, ev.DateofQuiz, ev.Questions = in.Nameofroom, in.StartTime, in.DateofQuiz, in.Questions
	ev.ScheduledAt, ev.AutoStart = in.ScheduledAt, in.AutoStart
	c.JSON(http.StatusOK, gin.H{"message": "saved", "event": ev})
}

func DeleteEvent(c *gin.Context) {
	ev, ok := owned(c, c.Param("roomid"))
	if !ok {
		return
	}
	if logicforuser.IsLive(ev.RoomId) {
		c.JSON(http.StatusConflict, gin.H{"error": "end the live room before deleting it"})
		return
	}
	ctx, cancel := timeout()
	defer cancel()
	if _, err := events().DeleteOne(ctx, bson.M{"roomid": ev.RoomId}); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "could not delete event"})
		return
	}
	_, _ = structure.DB().Collection("answers").DeleteMany(ctx, bson.M{"roomid": ev.RoomId})
	c.JSON(http.StatusOK, gin.H{"message": "deleted"})
}

// ResetEvent clears the players and answers of one of the admin's tests,
// so it can be run again from scratch. A live room goes back to its lobby.
// POST /api/admin/events/:roomid/reset
func ResetEvent(c *gin.Context) {
	ev, ok := owned(c, c.Param("roomid"))
	if !ok {
		return
	}
	players, removed, err := logicforuser.ResetRoom(ev.RoomId)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "could not reset players"})
		return
	}
	c.JSON(http.StatusOK, gin.H{"message": "reset", "players": players, "answers": removed, "live": logicforuser.IsLive(ev.RoomId)})
}

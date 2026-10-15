package logicforuser

import (
	"context"
	"encoding/json"
	"strconv"
	"sync"

	"trivyo_online/structure"

	"github.com/gin-gonic/gin"
	"github.com/redis/go-redis/v9"
)

const pointsPerCorrect = 50

type LeaderboardEntry struct {
	Rank   int    `json:"rank"`
	UserID string `json:"user_id"`
	Score  int    `json:"score"`
}

var (
	sharedRdb  *redis.Client
	sharedOnce sync.Once
)

func rdbClient() *redis.Client {
	sharedOnce.Do(func() { sharedRdb = structure.RedisClient() })
	return sharedRdb
}

func leaderboardKey(roomID string) string { return "leaderboard:" + roomID }

// AddScore awards points to a user and returns their new total.
func AddScore(ctx context.Context, rdb *redis.Client, roomID, userID string, points int) (float64, error) {
	return rdb.ZIncrBy(ctx, leaderboardKey(roomID), float64(points), userID).Result()
}

// MarkAnswered records that a user answered a question; false means they already had.
func MarkAnswered(ctx context.Context, rdb *redis.Client, roomID, qNo, userID string) (bool, error) {
	n, err := rdb.SAdd(ctx, "answered:"+roomID+":"+qNo, userID).Result()
	return n == 1, err
}

// GetLeaderboard returns the top `limit` players (all if limit <= 0), highest score first.
// Players with equal scores share a rank (1, 2, 2, 4).
func GetLeaderboard(ctx context.Context, rdb *redis.Client, roomID string, limit int) ([]LeaderboardEntry, error) {
	stop := int64(-1)
	if limit > 0 {
		stop = int64(limit - 1)
	}
	rows, err := rdb.ZRevRangeWithScores(ctx, leaderboardKey(roomID), 0, stop).Result()
	if err != nil {
		return nil, err
	}
	entries := make([]LeaderboardEntry, 0, len(rows))
	for i, z := range rows {
		rank := i + 1
		if i > 0 && int(z.Score) == entries[i-1].Score {
			rank = entries[i-1].Rank
		}
		entries = append(entries, LeaderboardEntry{Rank: rank, UserID: z.Member.(string), Score: int(z.Score)})
	}
	return entries, nil
}

// GET /api/leaderboard?roomid=...&limit=10&userid=...
func LeaderboardHandler(c *gin.Context) {
	roomID := c.Query("roomid")
	if roomID == "" {
		c.JSON(400, gin.H{"error": "missing roomid"})
		return
	}
	limit, _ := strconv.Atoi(c.Query("limit"))
	rdb := rdbClient()

	entries, err := GetLeaderboard(c.Request.Context(), rdb, roomID, limit)
	if err != nil {
		c.JSON(500, gin.H{"error": "cannot read leaderboard"})
		return
	}
	resp := gin.H{"room_id": roomID, "leaderboard": entries}

	// Optionally include the caller's own position even if outside the top N.
	if uid := c.Query("userid"); uid != "" {
		all, err := GetLeaderboard(c.Request.Context(), rdb, roomID, 0)
		if err == nil {
			for _, e := range all {
				if e.UserID == uid {
					resp["you"] = e
					break
				}
			}
		}
	}
	c.JSON(200, resp)
}

// broadcastLeaderboard pushes the current top 10 to all players and hosts in the room.
func broadcastLeaderboard(ctx context.Context, rdb *redis.Client, hub *Room) {
	entries, err := GetLeaderboard(ctx, rdb, hub.ID, 10)
	if err != nil {
		return
	}
	b, err := json.Marshal(gin.H{"type": "leaderboard", "leaderboard": entries})
	if err != nil {
		return
	}
	hub.Broadcast <- b
}

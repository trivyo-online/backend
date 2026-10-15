// Package auth handles admin (host) accounts: bcrypt-hashed passwords and
// HMAC-signed bearer tokens. An admin's username is their HostID.
package auth

import (
	"context"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"log"
	"net/http"
	"os"
	"regexp"
	"strings"
	"sync"
	"time"

	"trivyo_online/structure"

	"github.com/gin-gonic/gin"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"golang.org/x/crypto/bcrypt"
)

const tokenTTL = 7 * 24 * time.Hour

type Admin struct {
	Username     string    `bson:"username" json:"username"`
	PasswordHash string    `bson:"passwordhash" json:"-"`
	CreatedAt    time.Time `bson:"createdat" json:"createdat"`
}

type claims struct {
	Sub string `json:"sub"`
	Exp int64  `json:"exp"`
}

var (
	secretOnce sync.Once
	secret     []byte
)

func key() []byte {
	secretOnce.Do(func() {
		if s := os.Getenv("ADMIN_SECRET"); s != "" {
			secret = []byte(s)
			return
		}
		b := make([]byte, 32)
		_, _ = rand.Read(b)
		secret = b
		log.Println("ADMIN_SECRET not set: using a random key, so admin logins reset when the server restarts")
	})
	return secret
}

func Sign(username string) (string, error) {
	body, err := json.Marshal(claims{Sub: username, Exp: time.Now().Add(tokenTTL).Unix()})
	if err != nil {
		return "", err
	}
	p := base64.RawURLEncoding.EncodeToString(body)
	m := hmac.New(sha256.New, key())
	m.Write([]byte(p))
	return p + "." + hex.EncodeToString(m.Sum(nil)), nil
}

// Verify returns the username a token was issued to.
func Verify(token string) (string, error) {
	p, sig, ok := strings.Cut(token, ".")
	if !ok {
		return "", errors.New("malformed token")
	}
	m := hmac.New(sha256.New, key())
	m.Write([]byte(p))
	want := hex.EncodeToString(m.Sum(nil))
	if !hmac.Equal([]byte(sig), []byte(want)) {
		return "", errors.New("bad signature")
	}
	raw, err := base64.RawURLEncoding.DecodeString(p)
	if err != nil {
		return "", errors.New("malformed token")
	}
	var c claims
	if err := json.Unmarshal(raw, &c); err != nil || c.Sub == "" {
		return "", errors.New("malformed token")
	}
	if time.Now().Unix() > c.Exp {
		return "", errors.New("token expired")
	}
	return c.Sub, nil
}

// TokenFrom reads "Authorization: Bearer <t>", falling back to ?token= for
// WebSocket upgrades, where browsers cannot set headers.
func TokenFrom(c *gin.Context) string {
	if h := c.GetHeader("Authorization"); strings.HasPrefix(h, "Bearer ") {
		return strings.TrimPrefix(h, "Bearer ")
	}
	return c.Query("token")
}

// Require rejects requests without a valid admin token and stores the
// admin's username under "admin".
func Require() gin.HandlerFunc {
	return func(c *gin.Context) {
		user, err := Verify(TokenFrom(c))
		if err != nil {
			c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{"error": "login required"})
			return
		}
		c.Set("admin", user)
		c.Next()
	}
}

func Current(c *gin.Context) string {
	return c.GetString("admin")
}

var usernameRe = regexp.MustCompile(`^[a-zA-Z0-9_.-]{3,32}$`)

type credentials struct {
	Username string `json:"username"`
	Password string `json:"password"`
	Code     string `json:"code"`
}

func admins() *mongo.Collection {
	return structure.DB().Collection("admins")
}

func Signup(c *gin.Context) {
	var in credentials
	if err := c.BindJSON(&in); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid JSON"})
		return
	}
	if code := os.Getenv("ADMIN_SIGNUP_CODE"); code != "" && !hmac.Equal([]byte(in.Code), []byte(code)) {
		c.JSON(http.StatusForbidden, gin.H{"error": "invalid signup code"})
		return
	}
	if !usernameRe.MatchString(in.Username) {
		c.JSON(http.StatusBadRequest, gin.H{"error": "username must be 3-32 letters, digits, '.', '_' or '-'"})
		return
	}
	if len(in.Password) < 8 {
		c.JSON(http.StatusBadRequest, gin.H{"error": "password must be at least 8 characters"})
		return
	}
	hash, err := bcrypt.GenerateFromPassword([]byte(in.Password), bcrypt.DefaultCost)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "could not hash password"})
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err = admins().InsertOne(ctx, Admin{Username: in.Username, PasswordHash: string(hash), CreatedAt: time.Now()})
	if mongo.IsDuplicateKeyError(err) {
		c.JSON(http.StatusConflict, gin.H{"error": "username taken"})
		return
	}
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "could not create admin"})
		return
	}
	issue(c, in.Username)
}

func Login(c *gin.Context) {
	var in credentials
	if err := c.BindJSON(&in); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid JSON"})
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var a Admin
	err := admins().FindOne(ctx, bson.M{"username": in.Username}).Decode(&a)
	if err != nil || bcrypt.CompareHashAndPassword([]byte(a.PasswordHash), []byte(in.Password)) != nil {
		c.JSON(http.StatusUnauthorized, gin.H{"error": "wrong username or password"})
		return
	}
	issue(c, a.Username)
}

func Me(c *gin.Context) {
	c.JSON(http.StatusOK, gin.H{"username": Current(c)})
}

func issue(c *gin.Context, username string) {
	t, err := Sign(username)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "could not sign token"})
		return
	}
	c.JSON(http.StatusOK, gin.H{"token": t, "username": username})
}

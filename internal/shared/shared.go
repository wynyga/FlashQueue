// Package shared holds what the API and the worker must agree on: queue names and topology, the order
// message, Redis keys, and the one Lua script that decides an order's outcome.
package shared

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"log"
	"os"
	"strconv"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/redis/go-redis/v9"
)

const (
	QueueOrders = "orders"       // work waiting to be processed
	QueueRetry  = "orders.retry" // failed once; waits RetryDelayMs, then flows back into QueueOrders
	QueueDead   = "orders.dlq"   // gave up after MaxAttempts; kept for a human to look at

	RetryDelayMs = 3000
	MaxAttempts  = 3

	KeyStock = "stock"
	KeyStats = "stats"
)

// Order is the message on the queue. Everything else about an order lives in Redis under OrderKey(ID).
type Order struct {
	ID   string `json:"id"`
	User string `json:"user"`
	At   int64  `json:"at"`
}

func OrderKey(id string) string { return "order:" + id }
func IdemKey(key string) string { return "idem:" + key }

func NewID() string {
	b := make([]byte, 8)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}

func Getenv(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func GetenvInt(key string, def int) int {
	if v, err := strconv.Atoi(os.Getenv(key)); err == nil {
		return v
	}
	return def
}

// DeclareTopology creates the three queues. Declaring is idempotent, so both services call it at start-up.
// The retry queue has a TTL and dead-letters back into the main queue, which gives a delayed retry
// without any plugin: a message waits in orders.retry, expires, and lands in orders again.
func DeclareTopology(ch *amqp.Channel) error {
	if _, err := ch.QueueDeclare(QueueOrders, true, false, false, false, nil); err != nil {
		return err
	}
	if _, err := ch.QueueDeclare(QueueRetry, true, false, false, false, amqp.Table{
		"x-message-ttl":             int32(RetryDelayMs),
		"x-dead-letter-exchange":    "",
		"x-dead-letter-routing-key": QueueOrders,
	}); err != nil {
		return err
	}
	_, err := ch.QueueDeclare(QueueDead, true, false, false, false, nil)
	return err
}

// Process decides one order exactly once, however many times the message is delivered.
//
// RabbitMQ delivers at least once: if a worker dies after doing the work but before acknowledging, the
// message comes back. So the outcome is decided here, atomically, and a second run just returns the
// first result instead of taking stock again.
//
//	KEYS: order hash, stock, stats hash        returns: confirmed | sold_out | unknown
var Process = redis.NewScript(`
if redis.call('EXISTS', KEYS[1]) == 0 then return 'unknown' end
local st = redis.call('HGET', KEYS[1], 'status')
if st == 'confirmed' or st == 'sold_out' then return st end
local stock = tonumber(redis.call('GET', KEYS[2]) or '0')
if stock > 0 then
  redis.call('DECR', KEYS[2])
  redis.call('HSET', KEYS[1], 'status', 'confirmed')
  redis.call('HINCRBY', KEYS[3], 'confirmed', 1)
  return 'confirmed'
end
redis.call('HSET', KEYS[1], 'status', 'sold_out')
redis.call('HINCRBY', KEYS[3], 'sold_out', 1)
return 'sold_out'
`)

func RunProcess(ctx context.Context, rdb *redis.Client, id string) (string, error) {
	return Process.Run(ctx, rdb, []string{OrderKey(id), KeyStock, KeyStats}).Text()
}

// DialAMQP waits for the broker for up to a minute, since in Docker it can start after we do.
func DialAMQP(url string) *amqp.Connection {
	var err error
	for i := 0; i < 30; i++ {
		var conn *amqp.Connection
		if conn, err = amqp.Dial(url); err == nil {
			// Losing the broker connection is not worth limping along from: exit and let the
			// supervisor (docker restart policy) start a clean process that reconnects.
			go func() {
				e := <-conn.NotifyClose(make(chan *amqp.Error, 1))
				log.Fatalf("rabbitmq connection closed: %v", e)
			}()
			return conn
		}
		time.Sleep(2 * time.Second)
	}
	log.Fatalf("cannot reach rabbitmq: %v", err)
	return nil
}

func DialRedis(addr string) *redis.Client {
	rdb := redis.NewClient(&redis.Options{Addr: addr, PoolSize: 64})
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	for ctx.Err() == nil {
		if rdb.Ping(ctx).Err() == nil {
			return rdb
		}
		time.Sleep(time.Second)
	}
	log.Fatal(fmt.Sprintf("cannot reach redis at %s", addr))
	return nil
}

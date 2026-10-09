// The API: takes orders fast and puts them on a queue instead of doing the work while the client waits.
//
//	POST /orders        queue an order            -> 202 immediately (needs an Idempotency-Key header)
//	POST /orders/sync   the same work, no queue   -> waits for a free worker slot (the baseline to compare)
//	GET  /orders/{id}   one order's status
//	GET  /stats         stock, outcomes and queue depth
//	GET  /metrics       Prometheus metrics
package main

import (
	"context"
	"encoding/json"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"sync/atomic"
	"syscall"
	"time"

	"flashqueue/internal/shared"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/redis/go-redis/v9"
)

type server struct {
	rdb      *redis.Client
	pool     chan *amqp.Channel // confirm-mode channels; an AMQP channel must not be shared between goroutines
	maxQueue int64              // above this many waiting orders, new ones are turned away (backpressure)
	workMs   int
	syncSem  chan struct{} // limits the no-queue path to the same capacity a worker pool would have
	adminKey string

	depth, retrying, dead, consumers atomic.Int64

	orders  *prometheus.CounterVec
	latency *prometheus.HistogramVec
}

func main() {
	rdb := shared.DialRedis(shared.Getenv("REDIS_ADDR", "localhost:6379"))
	conn := shared.DialAMQP(shared.Getenv("AMQP_URL", "amqp://guest:guest@localhost:5672/"))

	s := &server{
		rdb:      rdb,
		pool:     make(chan *amqp.Channel, shared.GetenvInt("PUBLISH_CHANNELS", 8)),
		maxQueue: int64(shared.GetenvInt("MAX_QUEUE", 20000)),
		workMs:   shared.GetenvInt("WORK_MS", 50),
		syncSem:  make(chan struct{}, shared.GetenvInt("SYNC_CONCURRENCY", 8)),
		adminKey: shared.Getenv("ADMIN_KEY", ""),
		orders: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "flashqueue_api_orders_total", Help: "Order requests by outcome."}, []string{"result"}),
		latency: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name: "flashqueue_api_request_seconds", Help: "Request time by route.",
			Buckets: []float64{.001, .0025, .005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5}}, []string{"route"}),
	}
	queueDepth := prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "flashqueue_queue_depth", Help: "Messages waiting per queue."}, []string{"queue"})
	prometheus.MustRegister(s.orders, s.latency, queueDepth)

	setup, err := conn.Channel()
	if err != nil {
		log.Fatal(err)
	}
	if err := shared.DeclareTopology(setup); err != nil {
		log.Fatal(err)
	}
	if err := rdb.SetNX(context.Background(), shared.KeyStock, shared.GetenvInt("STOCK_INIT", 500), 0).Err(); err != nil {
		log.Fatal(err)
	}
	for i := 0; i < cap(s.pool); i++ {
		ch, err := conn.Channel()
		if err != nil {
			log.Fatal(err)
		}
		if err := ch.Confirm(false); err != nil {
			log.Fatal(err)
		}
		s.pool <- ch
	}

	go s.pollQueues(setup, queueDepth)

	mux := http.NewServeMux()
	mux.HandleFunc("POST /orders", s.timed("queue", s.createOrder(true)))
	mux.HandleFunc("POST /orders/sync", s.timed("sync", s.createOrder(false)))
	mux.HandleFunc("GET /orders/{id}", s.getOrder)
	mux.HandleFunc("GET /stats", s.stats)
	mux.HandleFunc("POST /admin/reset", s.reset(setup))
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusNoContent) })
	mux.Handle("GET /metrics", promhttp.Handler())

	srv := &http.Server{Addr: ":8080", Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	go func() {
		log.Printf("api listening on %s (work=%dms, max queue=%d)", srv.Addr, s.workMs, s.maxQueue)
		if err := srv.ListenAndServe(); err != http.ErrServerClosed {
			log.Fatal(err)
		}
	}()

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGINT, syscall.SIGTERM)
	<-stop
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_ = srv.Shutdown(ctx)
}

// pollQueues keeps the queue sizes fresh twice a second, so the hot path never has to ask the broker.
func (s *server) pollQueues(ch *amqp.Channel, gauge *prometheus.GaugeVec) {
	for range time.Tick(500 * time.Millisecond) {
		for _, q := range []struct {
			name string
			dst  *atomic.Int64
		}{{shared.QueueOrders, &s.depth}, {shared.QueueRetry, &s.retrying}, {shared.QueueDead, &s.dead}} {
			info, err := ch.QueueDeclarePassive(q.name, true, false, false, false, nil)
			if err != nil {
				log.Fatalf("queue %s: %v", q.name, err)
			}
			q.dst.Store(int64(info.Messages))
			gauge.WithLabelValues(q.name).Set(float64(info.Messages))
			if q.name == shared.QueueOrders {
				s.consumers.Store(int64(info.Consumers))
			}
		}
	}
}

func (s *server) timed(route string, h http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		h(w, r)
		s.latency.WithLabelValues(route).Observe(time.Since(start).Seconds())
	}
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

// createOrder is both order endpoints. They share the idempotency step and differ only in what happens
// next: queue it and answer now, or do the work inline and make the client wait for it.
func (s *server) createOrder(queued bool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		key := r.Header.Get("Idempotency-Key")
		if key == "" || len(key) > 128 {
			writeJSON(w, 400, map[string]string{"error": "send an Idempotency-Key header (1-128 characters)"})
			return
		}
		var body struct {
			User string `json:"user"`
		}
		_ = json.NewDecoder(http.MaxBytesReader(w, r.Body, 1024)).Decode(&body)
		if body.User == "" {
			body.User = "anonymous"
		}

		// Backpressure: when the backlog is already huge, say so now instead of promising work we can't do.
		if queued && s.depth.Load() > s.maxQueue {
			s.orders.WithLabelValues("rejected").Inc()
			w.Header().Set("Retry-After", "1")
			writeJSON(w, 503, map[string]string{"error": "too busy, try again shortly"})
			return
		}

		// The same key always maps to the same order, so a retry or double click never orders twice.
		id := shared.NewID()
		fresh, err := s.rdb.SetNX(ctx, shared.IdemKey(key), id, time.Hour).Result()
		if err != nil {
			writeJSON(w, 500, map[string]string{"error": "storage unavailable"})
			return
		}
		if !fresh {
			existing, _ := s.rdb.Get(ctx, shared.IdemKey(key)).Result()
			status, _ := s.rdb.HGet(ctx, shared.OrderKey(existing), "status").Result()
			s.orders.WithLabelValues("duplicate").Inc()
			_ = s.rdb.HIncrBy(ctx, shared.KeyStats, "duplicate", 1).Err()
			writeJSON(w, 200, map[string]any{"orderId": existing, "status": status, "duplicate": true})
			return
		}

		pipe := s.rdb.Pipeline()
		pipe.HSet(ctx, shared.OrderKey(id), "status", "queued", "user", body.User)
		pipe.HIncrBy(ctx, shared.KeyStats, "accepted", 1)
		if _, err := pipe.Exec(ctx); err != nil {
			s.rdb.Del(ctx, shared.IdemKey(key))
			writeJSON(w, 500, map[string]string{"error": "storage unavailable"})
			return
		}

		if queued {
			if err := s.publish(ctx, shared.Order{ID: id, User: body.User, At: time.Now().UnixMilli()}); err != nil {
				// Not on the queue, so undo everything: the client can retry with the same key.
				s.rdb.Del(ctx, shared.IdemKey(key), shared.OrderKey(id))
				_ = s.rdb.HIncrBy(ctx, shared.KeyStats, "accepted", -1).Err()
				s.orders.WithLabelValues("publish_failed").Inc()
				writeJSON(w, 503, map[string]string{"error": "could not queue the order, try again"})
				return
			}
			s.orders.WithLabelValues("accepted").Inc()
			writeJSON(w, 202, map[string]string{"orderId": id, "status": "queued"})
			return
		}

		// No queue: wait for one of a fixed number of slots, do the work, and only then answer.
		select {
		case s.syncSem <- struct{}{}:
			defer func() { <-s.syncSem }()
		case <-time.After(5 * time.Second):
			// Never processed, so undo the order entirely; the client can retry with the same key.
			s.rdb.Del(ctx, shared.IdemKey(key), shared.OrderKey(id))
			_ = s.rdb.HIncrBy(ctx, shared.KeyStats, "accepted", -1).Err()
			s.orders.WithLabelValues("timeout").Inc()
			writeJSON(w, 503, map[string]string{"error": "timed out waiting for capacity"})
			return
		}
		time.Sleep(time.Duration(s.workMs) * time.Millisecond)
		result, err := shared.RunProcess(ctx, s.rdb, id)
		if err != nil {
			writeJSON(w, 500, map[string]string{"error": "processing failed"})
			return
		}
		s.orders.WithLabelValues("sync_" + result).Inc()
		writeJSON(w, 200, map[string]string{"orderId": id, "status": result})
	}
}

// publish puts one order on the queue and waits for the broker to confirm it is stored, so a 202 means
// the order will not be lost even if the broker restarts.
func (s *server) publish(ctx context.Context, o shared.Order) error {
	ch := <-s.pool
	defer func() { s.pool <- ch }()
	body, _ := json.Marshal(o)
	ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	conf, err := ch.PublishWithDeferredConfirmWithContext(ctx, "", shared.QueueOrders, false, false, amqp.Publishing{
		DeliveryMode: amqp.Persistent, ContentType: "application/json", Body: body,
	})
	if err != nil {
		return err
	}
	acked, err := conf.WaitContext(ctx)
	if err != nil {
		return err
	}
	if !acked {
		return context.DeadlineExceeded
	}
	return nil
}

func (s *server) getOrder(w http.ResponseWriter, r *http.Request) {
	fields, err := s.rdb.HGetAll(r.Context(), shared.OrderKey(r.PathValue("id"))).Result()
	if err != nil {
		writeJSON(w, 500, map[string]string{"error": "storage unavailable"})
		return
	}
	if len(fields) == 0 {
		writeJSON(w, 404, map[string]string{"error": "no such order"})
		return
	}
	writeJSON(w, 200, map[string]string{"orderId": r.PathValue("id"), "status": fields["status"]})
}

func (s *server) stats(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	counters, _ := s.rdb.HGetAll(ctx, shared.KeyStats).Result()
	num := func(k string) int64 { n, _ := strconv.ParseInt(counters[k], 10, 64); return n }
	stock, _ := s.rdb.Get(ctx, shared.KeyStock).Int64()
	accepted, confirmed, soldOut, failed := num("accepted"), num("confirmed"), num("sold_out"), num("dead")
	writeJSON(w, 200, map[string]any{
		"stock":     stock,
		"accepted":  accepted,
		"confirmed": confirmed,
		"soldOut":   soldOut,
		"failed":    failed,
		"duplicate": num("duplicate"),
		// Orders accepted but not yet decided: waiting, being worked on, or waiting to retry.
		"pending":   accepted - confirmed - soldOut - failed,
		"queue":     map[string]int64{"waiting": s.depth.Load(), "retrying": s.retrying.Load(), "dead": s.dead.Load(), "workers": s.consumers.Load()},
	})
}

// reset puts the demo back to a clean slate (stock, orders, counters, queues). For tests and demos only.
func (s *server) reset(ch *amqp.Channel) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if s.adminKey == "" || r.Header.Get("X-Admin-Key") != s.adminKey {
			writeJSON(w, 403, map[string]string{"error": "forbidden"})
			return
		}
		stock := shared.GetenvInt("STOCK_INIT", 500)
		if v, err := strconv.Atoi(r.URL.Query().Get("stock")); err == nil && v >= 0 {
			stock = v
		}
		ctx := r.Context()
		for _, pattern := range []string{"order:*", "idem:*"} {
			iter := s.rdb.Scan(ctx, 0, pattern, 1000).Iterator()
			for iter.Next(ctx) {
				s.rdb.Del(ctx, iter.Val())
			}
		}
		s.rdb.Del(ctx, shared.KeyStats)
		s.rdb.Set(ctx, shared.KeyStock, stock, 0)
		for _, q := range []string{shared.QueueOrders, shared.QueueRetry, shared.QueueDead} {
			_, _ = ch.QueuePurge(q, false)
		}
		writeJSON(w, 200, map[string]any{"stock": stock})
	}
}

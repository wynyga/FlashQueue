// The worker: pulls orders off the queue and decides them, at a pace it can sustain.
//
// WORKERS goroutines share one consumer, and the broker hands each at most PREFETCH unacknowledged orders,
// so a burst piles up safely in the queue instead of overwhelming the workers. A failed order is retried
// after a delay and, after MaxAttempts, parked in the dead-letter queue.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"log"
	"math/rand"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"flashqueue/internal/shared"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/redis/go-redis/v9"
)

var errInjected = errors.New("injected failure")

type worker struct {
	rdb     *redis.Client
	pubMu   sync.Mutex // guards pub: one AMQP channel must not publish from several goroutines at once
	pub     *amqp.Channel
	workMs  int
	failPct float64

	processed *prometheus.CounterVec
	duration  prometheus.Histogram
}

func main() {
	rdb := shared.DialRedis(shared.Getenv("REDIS_ADDR", "localhost:6379"))
	conn := shared.DialAMQP(shared.Getenv("AMQP_URL", "amqp://guest:guest@localhost:5672/"))
	workers := shared.GetenvInt("WORKERS", 8)

	w := &worker{
		rdb:     rdb,
		workMs:  shared.GetenvInt("WORK_MS", 50),
		failPct: float64(shared.GetenvInt("FAIL_PCT", 0)) / 100,
		processed: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "flashqueue_worker_processed_total", Help: "Orders handled, by result."}, []string{"result"}),
		duration: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name: "flashqueue_worker_process_seconds", Help: "Time to handle one order.",
			Buckets: []float64{.005, .01, .025, .05, .1, .25, .5, 1, 2.5}}),
	}
	prometheus.MustRegister(w.processed, w.duration)

	ch, err := conn.Channel()
	if err != nil {
		log.Fatal(err)
	}
	if err := shared.DeclareTopology(ch); err != nil {
		log.Fatal(err)
	}
	if w.pub, err = conn.Channel(); err != nil {
		log.Fatal(err)
	}
	if err := w.pub.Confirm(false); err != nil {
		log.Fatal(err)
	}
	// The broker will not give a worker more than this many unacknowledged orders: this is the backpressure.
	if err := ch.Qos(workers*2, 0, false); err != nil {
		log.Fatal(err)
	}

	host, _ := os.Hostname()
	tag := "worker-" + host
	deliveries, err := ch.Consume(shared.QueueOrders, tag, false, false, false, false, nil)
	if err != nil {
		log.Fatal(err)
	}

	go func() {
		mux := http.NewServeMux()
		mux.Handle("/metrics", promhttp.Handler())
		log.Fatal(http.ListenAndServe(":9102", mux))
	}()

	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for d := range deliveries {
				w.handle(d)
			}
		}()
	}
	log.Printf("worker %s up: %d workers, work=%dms, fail=%.0f%%", host, workers, w.workMs, w.failPct*100)

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGINT, syscall.SIGTERM)
	<-stop
	// Stop taking new orders, then let the ones already in hand finish before exiting.
	_ = ch.Cancel(tag, false)
	wg.Wait()
}

func (w *worker) handle(d amqp.Delivery) {
	start := time.Now()
	defer func() { w.duration.Observe(time.Since(start).Seconds()) }()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	var o shared.Order
	if err := json.Unmarshal(d.Body, &o); err != nil {
		// A message that is not an order can never succeed: park it rather than retrying forever.
		w.bury(ctx, d, "")
		return
	}

	// Stand-in for real work (payment, email, ...): a little time, and sometimes a failure.
	time.Sleep(time.Duration(float64(w.workMs) * (0.7 + 0.6*rand.Float64()) * float64(time.Millisecond)))
	var result string
	var err error
	if rand.Float64() < w.failPct {
		err = errInjected
	} else {
		result, err = shared.RunProcess(ctx, w.rdb, o.ID)
	}

	if err != nil {
		w.retryOrBury(ctx, d, o.ID)
		return
	}
	w.processed.WithLabelValues(result).Inc()
	_ = d.Ack(false)
}

func attempts(d amqp.Delivery) int {
	switch v := d.Headers["x-attempts"].(type) {
	case int32:
		return int(v)
	case int64:
		return int(v)
	case int:
		return v
	}
	return 0
}

func (w *worker) retryOrBury(ctx context.Context, d amqp.Delivery, orderID string) {
	n := attempts(d) + 1
	if n >= shared.MaxAttempts {
		w.bury(ctx, d, orderID)
		return
	}
	if err := w.republish(ctx, shared.QueueRetry, d.Body, n); err != nil {
		_ = d.Nack(false, true) // could not park it for a retry: put it back so it is not lost
		return
	}
	w.processed.WithLabelValues("retried").Inc()
	_ = d.Ack(false)
}

func (w *worker) bury(ctx context.Context, d amqp.Delivery, orderID string) {
	if err := w.republish(ctx, shared.QueueDead, d.Body, attempts(d)+1); err != nil {
		_ = d.Nack(false, true)
		return
	}
	if orderID != "" {
		pipe := w.rdb.Pipeline()
		pipe.HSet(ctx, shared.OrderKey(orderID), "status", "failed")
		pipe.HIncrBy(ctx, shared.KeyStats, "dead", 1)
		_, _ = pipe.Exec(ctx)
	}
	w.processed.WithLabelValues("dead").Inc()
	_ = d.Ack(false)
}

// republish waits for the broker to confirm, so the original is only acknowledged once its copy is safe.
func (w *worker) republish(ctx context.Context, queue string, body []byte, n int) error {
	w.pubMu.Lock()
	defer w.pubMu.Unlock()
	conf, err := w.pub.PublishWithDeferredConfirmWithContext(ctx, "", queue, false, false, amqp.Publishing{
		DeliveryMode: amqp.Persistent, ContentType: "application/json", Body: body,
		Headers: amqp.Table{"x-attempts": int32(n)},
	})
	if err != nil {
		return err
	}
	acked, err := conf.WaitContext(ctx)
	if err != nil {
		return err
	}
	if !acked {
		return errors.New("broker did not confirm")
	}
	return nil
}

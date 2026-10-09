// check: fires a burst of orders at the API, waits for the system to settle, then verifies the rules that
// must hold no matter how busy it gets. It doubles as a small load generator that reports what it measured.
//
//	go run ./cmd/check                       # queue path
//	go run ./cmd/check -sync                 # the no-queue baseline, for comparison
//	go run ./cmd/check -n 20000 -c 500 -stock 100
package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"math/rand"
	"net/http"
	"os"
	"sort"
	"sync"
	"time"
)

type result struct {
	id     string
	status int
	took   time.Duration
}

func main() {
	url := flag.String("url", "http://localhost:8081", "API base URL")
	admin := flag.String("admin", "dev-admin", "ADMIN_KEY of the API (used to reset between runs)")
	n := flag.Int("n", 3000, "number of distinct orders to send")
	conc := flag.Int("c", 200, "concurrent clients")
	stock := flag.Int("stock", 100, "units in stock")
	dupes := flag.Float64("dupes", 0.05, "extra requests that repeat an earlier Idempotency-Key, as a fraction of n")
	sync_ := flag.Bool("sync", false, "use POST /orders/sync (no queue)")
	flag.Parse()

	client := &http.Client{
		Timeout:   30 * time.Second,
		Transport: &http.Transport{MaxIdleConns: 1000, MaxIdleConnsPerHost: 1000},
	}
	path := "/orders"
	if *sync_ {
		path = "/orders/sync"
	}
	run := fmt.Sprintf("%d", time.Now().UnixNano()) // keeps keys unique across runs

	// Start from a clean slate.
	req, _ := http.NewRequest("POST", fmt.Sprintf("%s/admin/reset?stock=%d", *url, *stock), nil)
	req.Header.Set("X-Admin-Key", *admin)
	if resp, err := client.Do(req); err != nil || resp.StatusCode != 200 {
		fail("could not reset the API (is it up, and is -admin right?): %v", err)
	}
	time.Sleep(time.Second) // let anything in flight from a previous run drain

	post := func(key string) result {
		start := time.Now()
		req, _ := http.NewRequest("POST", *url+path, bytes.NewReader([]byte(`{"user":"check"}`)))
		req.Header.Set("Idempotency-Key", key)
		resp, err := client.Do(req)
		if err != nil {
			return result{status: 0, took: time.Since(start)}
		}
		defer resp.Body.Close()
		var out struct {
			OrderID string `json:"orderId"`
		}
		_ = json.NewDecoder(resp.Body).Decode(&out)
		return result{id: out.OrderID, status: resp.StatusCode, took: time.Since(start)}
	}

	// Phase 1: the burst. n distinct orders from `conc` clients at once.
	fmt.Printf("burst: %d orders, %d concurrent clients, stock %d, path %s\n", *n, *conc, *stock, path)
	results := make([]result, *n)
	var wg sync.WaitGroup
	jobs := make(chan int)
	burstStart := time.Now()
	for i := 0; i < *conc; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := range jobs {
				results[j] = post(fmt.Sprintf("%s-%d", run, j))
			}
		}()
	}
	for j := 0; j < *n; j++ {
		jobs <- j
	}
	close(jobs)
	wg.Wait()
	burst := time.Since(burstStart)

	accepted, rejected, broken := 0, 0, 0
	ids := map[int]string{}
	var lat []time.Duration
	for j, r := range results {
		switch r.status {
		case 200, 202:
			accepted++
			ids[j] = r.id
			lat = append(lat, r.took)
		case 503:
			rejected++
		default:
			broken++
		}
	}

	// Phase 2: repeat some keys. Each must hand back the order it created the first time.
	dupCount := int(float64(*n) * *dupes)
	wrongDup := 0
	var dmu sync.Mutex
	var dwg sync.WaitGroup
	for i := 0; i < dupCount; i++ {
		j := rand.Intn(*n)
		want, ok := ids[j]
		if !ok {
			continue
		}
		dwg.Add(1)
		go func() {
			defer dwg.Done()
			if got := post(fmt.Sprintf("%s-%d", run, j)); got.id != want {
				dmu.Lock()
				wrongDup++
				dmu.Unlock()
			}
		}()
	}
	dwg.Wait()

	// Phase 3: wait for the backlog to drain.
	drainStart := time.Now()
	var st stats
	for time.Since(drainStart) < 5*time.Minute {
		st = getStats(client, *url)
		if st.Pending <= 0 && st.Queue.Waiting == 0 && st.Queue.Retrying == 0 {
			break
		}
		time.Sleep(250 * time.Millisecond)
	}
	drain := time.Since(drainStart)

	sort.Slice(lat, func(i, j int) bool { return lat[i] < lat[j] })
	pct := func(p float64) time.Duration {
		if len(lat) == 0 {
			return 0
		}
		return lat[int(float64(len(lat)-1)*p)]
	}

	fmt.Printf("\nsent:      %d accepted, %d turned away (503), %d errors\n", accepted, rejected, broken)
	fmt.Printf("speed:     %.0f requests/s accepted over %s\n", float64(accepted)/burst.Seconds(), burst.Round(time.Millisecond))
	fmt.Printf("latency:   p50 %s   p95 %s   p99 %s   (time to get an answer)\n", pct(.5).Round(time.Microsecond), pct(.95).Round(time.Microsecond), pct(.99).Round(time.Microsecond))
	fmt.Printf("backlog:   cleared %s after the burst ended\n", drain.Round(time.Millisecond))
	fmt.Printf("outcome:   confirmed %d, sold out %d, failed %d, still pending %d\n\n", st.Confirmed, st.SoldOut, st.Failed, st.Pending)

	// The rules that must hold.
	ok := true
	check := func(pass bool, what string) {
		mark := "PASS"
		if !pass {
			mark, ok = "FAIL", false
		}
		fmt.Printf("  [%s] %s\n", mark, what)
	}
	want := min(*stock, int(st.Accepted-st.Failed))
	check(st.Pending == 0, "every accepted order was decided (nothing lost or stuck)")
	check(st.Confirmed+st.SoldOut+st.Failed == st.Accepted, "confirmed + sold out + failed adds up to accepted")
	check(st.Confirmed <= int64(*stock), fmt.Sprintf("never oversold: %d confirmed for %d in stock", st.Confirmed, *stock))
	check(st.Stock == int64(*stock)-st.Confirmed, fmt.Sprintf("stock left (%d) matches what was confirmed", st.Stock))
	check(st.Confirmed == int64(want), fmt.Sprintf("all stock sold when demand allowed it (expected %d confirmed)", want))
	check(wrongDup == 0, fmt.Sprintf("a repeated Idempotency-Key never created a second order (%d repeats tried)", dupCount))
	check(broken == 0, "no unexpected errors")
	if !ok {
		os.Exit(1)
	}
}

type stats struct {
	Stock     int64 `json:"stock"`
	Accepted  int64 `json:"accepted"`
	Confirmed int64 `json:"confirmed"`
	SoldOut   int64 `json:"soldOut"`
	Failed    int64 `json:"failed"`
	Pending   int64 `json:"pending"`
	Queue     struct {
		Waiting  int64 `json:"waiting"`
		Retrying int64 `json:"retrying"`
	} `json:"queue"`
}

func getStats(c *http.Client, url string) (s stats) {
	resp, err := c.Get(url + "/stats")
	if err != nil {
		return
	}
	defer resp.Body.Close()
	_ = json.NewDecoder(resp.Body).Decode(&s)
	return
}

func fail(format string, args ...any) {
	fmt.Fprintf(os.Stderr, format+"\n", args...)
	os.Exit(1)
}

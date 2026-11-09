// Command stampede shows the failure and then the fix: the same load
// against a cache with no defences and against one with all three.
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"text/tabwriter"
	"time"

	"github.com/usmanvirk/cache-stampede/internal/cachet"
)

func main() {
	var (
		redisURL = flag.String("redis-url", os.Getenv("REDIS_URL"), "Redis DSN, empty for an in process store")
		callers  = flag.Int("callers", 2000, "concurrent readers")
		keys     = flag.Int("keys", 1, "how many distinct keys they read")
		originMS = flag.Int("origin-ms", 50, "how long the origin takes")
		ttl      = flag.Duration("ttl", 2*time.Second, "cache ttl")
	)
	flag.Parse()

	store, name := openStore(*redisURL)
	defer store.Close()

	fmt.Printf("%d callers, %d key(s), a %dms origin, a %s ttl, through %s\n\n",
		*callers, *keys, *originMS, *ttl, name)

	w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	fmt.Fprintln(w, "run\torigin calls\thits\tcollapsed\tserved stale\twall time")

	naive := runRound(store, "naive", *callers, *keys, *originMS, *ttl, false)
	row(w, "no defences", naive)

	guarded := runRound(store, "guarded", *callers, *keys, *originMS, *ttl, true)
	row(w, "collapsed", guarded)

	w.Flush()

	fmt.Printf("\nthe origin was called %d times instead of %d\n",
		guarded.calls, naive.calls)
}

type result struct {
	calls       int64
	hits        int64
	collapsed   int64
	servedStale int64
	wall        time.Duration
}

func row(w *tabwriter.Writer, label string, r result) {
	fmt.Fprintf(w, "%s\t%d\t%d\t%d\t%d\t%s\n",
		label, r.calls, r.hits, r.collapsed, r.servedStale, r.wall.Round(time.Millisecond))
}

// runRound drives the load. With collapse off, every miss calls the origin
// directly, which is what the code looked like before this package existed.
func runRound(store cachet.Store, prefix string, callers, keys, originMS int, ttl time.Duration, collapse bool) result {
	var calls atomic.Int64
	origin := func(ctx context.Context, key string) ([]byte, error) {
		calls.Add(1)
		time.Sleep(time.Duration(originMS) * time.Millisecond)
		return []byte("value for " + key), nil
	}

	c := &cachet.Cache{Store: store, Origin: origin, TTL: ttl}

	start := time.Now()
	var wg sync.WaitGroup
	for i := 0; i < callers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			key := fmt.Sprintf("%s:%d:%d", prefix, time.Now().UnixNano()/1e9, i%keys)
			if collapse {
				_, _ = c.Get(context.Background(), key)
				return
			}
			// The naive path: read the cache, and on a miss go straight to
			// the origin with no coordination at all.
			if _, found, _ := store.Get(context.Background(), key); found {
				return
			}
			_, _ = origin(context.Background(), key)
		}(i)
	}
	wg.Wait()
	c.Wait()

	return result{
		calls:       calls.Load(),
		hits:        c.Stats().Hits.Load(),
		collapsed:   c.Stats().Collapsed.Load(),
		servedStale: c.Stats().ServedStale.Load(),
		wall:        time.Since(start),
	}
}

func openStore(url string) (cachet.Store, string) {
	if url == "" {
		return cachet.NewMemory(), "an in process store"
	}
	r, err := cachet.NewRedis(url)
	if err != nil {
		fmt.Fprintln(os.Stderr, "stampede:", err)
		os.Exit(1)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := r.Ping(ctx); err != nil {
		fmt.Fprintln(os.Stderr, "stampede: redis did not answer:", err)
		os.Exit(1)
	}
	return r, "redis"
}

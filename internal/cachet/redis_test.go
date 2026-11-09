package cachet_test

import (
	"context"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/usmanvirk/cache-stampede/internal/cachet"
)

// A real Redis or nothing. What is worth testing here is that an entry
// survives the round trip through another process and that the physical
// TTL behaves as the stale window needs it to, and a mock of Redis would
// be a mock of exactly that.
func redisStore(t *testing.T) *cachet.Redis {
	t.Helper()
	addr := os.Getenv("REDIS_URL")
	if addr == "" {
		t.Skip("REDIS_URL is not set")
	}
	r, err := cachet.NewRedis(addr)
	if err != nil {
		t.Fatalf("connecting: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := r.Ping(ctx); err != nil {
		t.Fatalf("pinging redis: %v", err)
	}
	t.Cleanup(func() { _ = r.Close() })
	return r
}

func key(t *testing.T) string {
	return fmt.Sprintf("stampede:%s:%d", t.Name(), time.Now().UnixNano())
}

func TestRedisRoundTrip(t *testing.T) {
	r := redisStore(t)
	ctx := context.Background()
	k := key(t)

	if _, found, err := r.Get(ctx, k); err != nil || found {
		t.Fatalf("a fresh key reported found=%v err=%v", found, err)
	}
	if err := r.Set(ctx, k, []byte("hello"), time.Minute); err != nil {
		t.Fatal(err)
	}
	got, found, err := r.Get(ctx, k)
	if err != nil || !found || string(got) != "hello" {
		t.Fatalf("got %q found=%v err=%v", got, found, err)
	}
}

// TestTheStampedeIsPreventedThroughRedis. The collapse happens in process,
// so this checks the whole thing end to end against the real store rather
// than only the group.
func TestTheStampedeIsPreventedThroughRedis(t *testing.T) {
	r := redisStore(t)
	k := key(t)

	var calls atomic.Int64
	c := &cachet.Cache{
		Store: r,
		TTL:   time.Minute,
		Origin: func(ctx context.Context, _ string) ([]byte, error) {
			calls.Add(1)
			time.Sleep(50 * time.Millisecond)
			return []byte("value"), nil
		},
	}

	const callers = 500
	var wg sync.WaitGroup
	start := make(chan struct{})
	errs := make(chan error, callers)

	for i := 0; i < callers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			v, err := c.Get(context.Background(), k)
			if err != nil {
				errs <- err
				return
			}
			if string(v) != "value" {
				errs <- fmt.Errorf("got %q", v)
			}
		}()
	}
	close(start)
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}

	if got := calls.Load(); got != 1 {
		t.Fatalf("%d callers produced %d origin calls through redis, want 1", callers, got)
	}
}

// TestTheEntrySurvivesAnotherProcess. The value is decoded by a different
// Cache from the one that wrote it, which is what two replicas are.
func TestTheEntrySurvivesAnotherProcess(t *testing.T) {
	r := redisStore(t)
	k := key(t)
	ctx := context.Background()

	var writes atomic.Int64
	origin := func(context.Context, string) ([]byte, error) {
		writes.Add(1)
		return []byte("shared"), nil
	}

	first := &cachet.Cache{Store: r, Origin: origin, TTL: time.Minute}
	if _, err := first.Get(ctx, k); err != nil {
		t.Fatal(err)
	}

	second := &cachet.Cache{Store: r, Origin: origin, TTL: time.Minute}
	v, err := second.Get(ctx, k)
	if err != nil {
		t.Fatal(err)
	}
	if string(v) != "shared" {
		t.Fatalf("the second replica got %q", v)
	}
	if writes.Load() != 1 {
		t.Fatalf("%d origin calls, want 1: the second replica did not read the first one's entry", writes.Load())
	}
	if second.Stats().Hits.Load() != 1 {
		t.Fatal("the second replica recorded a miss on a warm key")
	}
}

// TestThePhysicalTTLOutlivesTheLogicalOne. Serving a stale value requires
// the value to still be in Redis after its logical deadline, so the Set
// has to carry the longer lifetime.
func TestThePhysicalTTLOutlivesTheLogicalOne(t *testing.T) {
	r := redisStore(t)
	k := key(t)
	ctx := context.Background()

	c := &cachet.Cache{
		Store: r, TTL: time.Second, StaleFor: time.Hour,
		Origin: func(context.Context, string) ([]byte, error) { return []byte("v"), nil },
	}
	if _, err := c.Get(ctx, k); err != nil {
		t.Fatal(err)
	}

	// The logical deadline is a second away. The entry must still be in
	// the store well past it, or there is nothing to serve stale.
	time.Sleep(1200 * time.Millisecond)

	if _, found, err := r.Get(ctx, k); err != nil || !found {
		t.Fatalf("the entry was gone %s after a %s logical ttl, so a stale read is impossible: found=%v err=%v",
			"1.2s", "1s", found, err)
	}
}

package cachet_test

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/usmanvirk/cache-stampede/internal/cachet"
)

// slowOrigin counts calls and takes a while, which is what makes a
// stampede a stampede: if the origin were fast there would be no herd.
type slowOrigin struct {
	calls atomic.Int64
	delay time.Duration
	fail  atomic.Bool
	value atomic.Int64

	// cost is how far the test clock moves while the origin works, which
	// is what the cache records as the value's delta. It is separate from
	// delay, the real sleep, so a test can describe a five second query
	// without taking five seconds.
	//
	// With a frozen clock every call appears to cost nothing, and a value
	// that cost nothing is never worth recomputing early, so the whole
	// scheme silently switches itself off. That is worth knowing: the
	// early refresh only ever helps where the origin is slow.
	cost    time.Duration
	advance func(time.Duration)
}

func (o *slowOrigin) fn(ctx context.Context, key string) ([]byte, error) {
	o.calls.Add(1)
	if o.delay > 0 {
		select {
		case <-time.After(o.delay):
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	if o.advance != nil && o.cost > 0 {
		o.advance(o.cost)
	}
	if o.fail.Load() {
		return nil, errors.New("origin is down")
	}
	return []byte(fmt.Sprintf("%s=%d", key, o.value.Load())), nil
}

// advance moves the shared test clock. The cache and the store both read
// it through the same closure, so moving it here moves it for both.
func advance(clock *time.Time, c *cachet.Cache, d time.Duration) {
	c.Wait()
	*clock = clock.Add(d)
}

func newCache(t *testing.T, o *slowOrigin, ttl time.Duration) (*cachet.Cache, *cachet.Memory, *time.Time) {
	t.Helper()
	store := cachet.NewMemory()
	now := time.Date(2026, 10, 23, 12, 0, 0, 0, time.UTC)
	clock := &now
	var clockMu sync.Mutex
	store.SetClock(func() time.Time { clockMu.Lock(); defer clockMu.Unlock(); return *clock })
	if o.advance == nil {
		o.advance = func(d time.Duration) { clockMu.Lock(); *clock = clock.Add(d); clockMu.Unlock() }
	}

	c := &cachet.Cache{
		Store: store, Origin: o.fn, TTL: ttl, StaleFor: 10 * time.Minute,
		Now:  func() time.Time { clockMu.Lock(); defer clockMu.Unlock(); return *clock },
		Rand: seeded(1),
	}
	return c, store, clock
}

// seeded returns a Rand factory that is deterministic across a run and
// still produces different values on each call.
//
// Returning rand.New(rand.NewSource(1)) from the factory looks deterministic
// and is useless: every caller gets the same first draw, so a probability
// that should fire one time in eight fires never. The master source is
// behind a mutex because math/rand's Rand is not safe for concurrent use
// and the race detector says so immediately.
func seeded(seed int64) func() *rand.Rand {
	var mu sync.Mutex
	master := rand.New(rand.NewSource(seed))
	return func() *rand.Rand {
		mu.Lock()
		defer mu.Unlock()
		return rand.New(rand.NewSource(master.Int63()))
	}
}

// TestTwoThousandCallersMakeOneOriginCall is the whole repository in one
// test. A popular key is cold and two thousand requests arrive together.
func TestTwoThousandCallersMakeOneOriginCall(t *testing.T) {
	o := &slowOrigin{delay: 30 * time.Millisecond}
	c, _, _ := newCache(t, o, time.Minute)

	const callers = 2000
	var wg sync.WaitGroup
	start := make(chan struct{})
	errs := make(chan error, callers)

	for i := 0; i < callers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			v, err := c.Get(context.Background(), "popular")
			if err != nil {
				errs <- err
				return
			}
			if string(v) != "popular=0" {
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

	if got := o.calls.Load(); got != 1 {
		t.Fatalf("%d callers produced %d origin calls, want 1", callers, got)
	}
	if collapsed := c.Stats().Collapsed.Load(); collapsed < callers-10 {
		t.Fatalf("only %d of %d callers were collapsed", collapsed, callers)
	}
}

// TestDifferentKeysAreNotCollapsed. Collapsing by key and not globally is
// the difference between a cache and a mutex around the database.
func TestDifferentKeysAreNotCollapsed(t *testing.T) {
	o := &slowOrigin{delay: 20 * time.Millisecond}
	c, _, _ := newCache(t, o, time.Minute)

	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			if _, err := c.Get(context.Background(), fmt.Sprintf("key%d", i)); err != nil {
				t.Error(err)
			}
		}(i)
	}
	wg.Wait()

	if got := o.calls.Load(); got != 20 {
		t.Fatalf("20 distinct keys produced %d origin calls, want 20", got)
	}
}

func TestASecondCallIsAHit(t *testing.T) {
	o := &slowOrigin{}
	c, _, _ := newCache(t, o, time.Minute)
	ctx := context.Background()

	if _, err := c.Get(ctx, "k"); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Get(ctx, "k"); err != nil {
		t.Fatal(err)
	}
	if got := o.calls.Load(); got != 1 {
		t.Fatalf("%d origin calls for two reads of one key", got)
	}
	if h := c.Stats().Hits.Load(); h != 1 {
		t.Fatalf("%d hits recorded", h)
	}
}

// TestAStaleValueIsServedWhileItRefreshes. The caller that finds an expired
// value should not be the one who waits for the origin.
func TestAStaleValueIsServedWhileItRefreshes(t *testing.T) {
	o := &slowOrigin{}
	c, _, clock := newCache(t, o, time.Minute)
	ctx := context.Background()

	if _, err := c.Get(ctx, "k"); err != nil {
		t.Fatal(err)
	}
	o.value.Store(1) // the origin now has a newer answer

	advance(clock, c, 2*time.Minute) // past the logical deadline

	v, err := c.Get(ctx, "k")
	if err != nil {
		t.Fatal(err)
	}
	if string(v) != "k=0" {
		t.Fatalf("got %q, want the stale value served immediately", v)
	}
	if n := c.Stats().ServedStale.Load(); n != 1 {
		t.Fatalf("served stale %d times", n)
	}

	c.Wait() // the background refresh

	v, err = c.Get(ctx, "k")
	if err != nil {
		t.Fatal(err)
	}
	if string(v) != "k=1" {
		t.Fatalf("after the refresh the value is %q, want the new one", v)
	}
}

// TestTheOriginBeingDownIsSurvivable. A stale price is better than an
// error page, for as long as the stale window allows.
func TestTheOriginBeingDownIsSurvivable(t *testing.T) {
	o := &slowOrigin{}
	c, _, clock := newCache(t, o, time.Minute)
	ctx := context.Background()

	if _, err := c.Get(ctx, "k"); err != nil {
		t.Fatal(err)
	}
	o.fail.Store(true)
	advance(clock, c, 2*time.Minute)

	for i := 0; i < 5; i++ {
		v, err := c.Get(ctx, "k")
		if err != nil {
			t.Fatalf("read %d failed while a stale value was available: %v", i, err)
		}
		if string(v) != "k=0" {
			t.Fatalf("got %q", v)
		}
	}
	c.Wait()

	if c.Stats().OriginErrors.Load() == 0 {
		t.Fatal("the origin never actually failed, so this test proves nothing")
	}
}

// TestPastTheStaleWindowTheErrorSurfaces. Serving a value forever is worse
// than failing: the stale window is a promise about how wrong the answer
// may be.
func TestPastTheStaleWindowTheErrorSurfaces(t *testing.T) {
	o := &slowOrigin{}
	c, _, clock := newCache(t, o, time.Minute)
	ctx := context.Background()

	if _, err := c.Get(ctx, "k"); err != nil {
		t.Fatal(err)
	}
	o.fail.Store(true)
	// Past the ttl and the whole stale window, so the store has dropped it.
	advance(clock, c, time.Minute+11*time.Minute)

	if _, err := c.Get(ctx, "k"); err == nil {
		t.Fatal("a value past its stale window was still served")
	}
}

// TestEarlyRefreshHappensBeforeTheValueGoesCold, which is the point of the
// probabilistic scheme: the key is refreshed while it is still valid, so
// nobody ever meets it cold.
func TestEarlyRefreshHappensBeforeTheValueGoesCold(t *testing.T) {
	// A five second query under a one minute ttl. The ttl is jittered by
	// up to ten percent, so the earliest it can expire is 54 seconds.
	o := &slowOrigin{cost: 5 * time.Second}
	c, _, clock := newCache(t, o, time.Minute)
	ctx := context.Background()

	if _, err := c.Get(ctx, "k"); err != nil {
		t.Fatal(err)
	}
	c.Wait()
	before := o.calls.Load()

	// 53 seconds in, so at least a second of life remains whatever the
	// jitter chose. With a delta of five seconds the chance of recomputing
	// is above eighty percent per read.
	advance(clock, c, 53*time.Second)

	found := false
	for i := 0; i < 50; i++ {
		if _, err := c.Get(ctx, "k"); err != nil {
			t.Fatal(err)
		}
		if c.Stats().EarlyRefresh.Load() > 0 {
			found = true
			break
		}
	}
	c.Wait()

	if !found {
		t.Fatal("fifty reads a second before the deadline triggered no early refresh")
	}
	if o.calls.Load() <= before {
		t.Fatal("an early refresh was counted but the origin was never called")
	}
	// And none of those reads found the value stale, which is the property
	// worth having: the key was refreshed before anybody missed it.
	if n := c.Stats().ServedStale.Load(); n != 0 {
		t.Fatalf("the value went cold %d times despite the early refresh", n)
	}
}

func TestEarlyRefreshDoesNotFireEarlyInTheLife(t *testing.T) {
	o := &slowOrigin{cost: 10 * time.Millisecond}
	c, _, clock := newCache(t, o, time.Hour)
	ctx := context.Background()

	if _, err := c.Get(ctx, "k"); err != nil {
		t.Fatal(err)
	}
	c.Wait()

	// One minute into a one hour life, with a ten millisecond delta. The
	// probability here should be vanishing.
	advance(clock, c, time.Minute)
	for i := 0; i < 200; i++ {
		if _, err := c.Get(ctx, "k"); err != nil {
			t.Fatal(err)
		}
	}
	c.Wait()
	if n := c.Stats().EarlyRefresh.Load(); n > 0 {
		t.Fatalf("%d early refreshes a minute into a one hour ttl, which is wasted origin load", n)
	}
}

// TestJitterSpreadsKeysWrittenTogether. This is the defence the other two
// do not provide: a thousand different keys written in one second with one
// TTL expire in one second, and collapsing by key does nothing about it.
func TestJitterSpreadsKeysWrittenTogether(t *testing.T) {
	rnd := rand.New(rand.NewSource(7))
	const ttl = 300 * time.Second

	seen := map[time.Duration]int{}
	var min, max time.Duration = 1 << 62, 0
	for i := 0; i < 1000; i++ {
		got := cachet.Jitter(ttl, rnd)
		seen[got.Round(time.Second)]++
		if got < min {
			min = got
		}
		if got > max {
			max = got
		}
	}

	if len(seen) < 50 {
		t.Fatalf("a thousand keys landed on %d distinct seconds, which is still a cliff", len(seen))
	}
	// Plus or minus 10%, so the ttl still means roughly what it says.
	if min < 265*time.Second || max > 335*time.Second {
		t.Fatalf("jitter ranged from %s to %s, outside the intended 10%%", min, max)
	}
}

func TestJitterLeavesANonPositiveTTLAlone(t *testing.T) {
	rnd := rand.New(rand.NewSource(1))
	if got := cachet.Jitter(0, rnd); got != 0 {
		t.Fatalf("jittered a zero ttl to %s", got)
	}
}

// TestAnUnreadableEntryIsTreatedAsAMiss. Otherwise a change to the stored
// shape means a manual cache flush during a deploy.
func TestAnUnreadableEntryIsTreatedAsAMiss(t *testing.T) {
	o := &slowOrigin{}
	c, store, _ := newCache(t, o, time.Minute)
	ctx := context.Background()

	if err := store.Set(ctx, "k", []byte("this is not an entry"), time.Hour); err != nil {
		t.Fatal(err)
	}
	v, err := c.Get(ctx, "k")
	if err != nil {
		t.Fatalf("an unreadable entry failed the request: %v", err)
	}
	if string(v) != "k=0" {
		t.Fatalf("got %q", v)
	}
}

// TestABrokenStoreIsNotAnOutage.
func TestABrokenStoreIsNotAnOutage(t *testing.T) {
	o := &slowOrigin{}
	c := &cachet.Cache{
		Store: brokenStore{}, Origin: o.fn, TTL: time.Minute,
		Rand: seeded(2),
	}
	v, err := c.Get(context.Background(), "k")
	if err != nil {
		t.Fatalf("a broken cache failed the request: %v", err)
	}
	if string(v) != "k=0" {
		t.Fatalf("got %q", v)
	}
}

type brokenStore struct{}

func (brokenStore) Get(context.Context, string) ([]byte, bool, error) {
	return nil, false, errors.New("redis is down")
}
func (brokenStore) Set(context.Context, string, []byte, time.Duration) error {
	return errors.New("redis is down")
}
func (brokenStore) Close() error { return nil }

// TestACallerThatGivesUpDoesNotBlockTheOthers.
func TestACallerThatGivesUpDoesNotStrandTheRest(t *testing.T) {
	o := &slowOrigin{delay: 200 * time.Millisecond}
	c, _, _ := newCache(t, o, time.Minute)

	// One caller starts the work and stays.
	done := make(chan error, 1)
	go func() {
		_, err := c.Get(context.Background(), "k")
		done <- err
	}()

	time.Sleep(20 * time.Millisecond)

	// A second gives up almost immediately.
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	if _, err := c.Get(ctx, "k"); err == nil {
		t.Fatal("a caller whose context expired got a value anyway")
	}

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("the first caller failed because a waiter left: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the first caller never finished")
	}
}

func TestOriginErrorsReachTheCaller(t *testing.T) {
	o := &slowOrigin{}
	o.fail.Store(true)
	c, _, _ := newCache(t, o, time.Minute)

	if _, err := c.Get(context.Background(), "k"); err == nil {
		t.Fatal("a cold key with a failing origin returned no error")
	}
}

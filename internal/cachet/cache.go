package cachet

import (
	"context"
	"errors"
	"log/slog"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"github.com/usmanvirk/cache-stampede/internal/single"
)

// Store is the bytes behind the cache.
//
// The physical TTL it is given is longer than the logical one in the entry,
// because serving a stale value requires the value to still be there.
type Store interface {
	Get(ctx context.Context, key string) ([]byte, bool, error)
	Set(ctx context.Context, key string, value []byte, ttl time.Duration) error
	Close() error
}

// Origin is the expensive thing behind the cache.
type Origin func(ctx context.Context, key string) ([]byte, error)

// StaleFor is how much longer than its logical TTL a value is physically
// kept, so it can be served while a refresh is in flight or while the
// origin is down.
//
// Ten minutes is a judgement: long enough to cover an origin outage of the
// length somebody can respond to, short enough that nothing is served from
// a value nobody would recognise. It is per cache rather than a constant
// because the right answer depends on how wrong a stale value can be, and
// a price is not a product description.
const DefaultStaleFor = 10 * time.Minute

type Stats struct {
	Hits         atomic.Int64
	Misses       atomic.Int64
	EarlyRefresh atomic.Int64
	ServedStale  atomic.Int64
	OriginCalls  atomic.Int64
	OriginErrors atomic.Int64
	Collapsed    atomic.Int64
}

// Cache is a read-through cache with all three defences.
type Cache struct {
	Store    Store
	Origin   Origin
	TTL      time.Duration
	StaleFor time.Duration
	Log      *slog.Logger

	// Now and Rand are replaceable so the tests can move time and remove
	// the randomness, rather than sleeping and hoping.
	Now  func() time.Time
	Rand func() *rand.Rand

	group *single.Group[[]byte]
	stats Stats

	once sync.Once
	// refreshing tracks background refreshes so a test can wait for them
	// instead of sleeping.
	refresh sync.WaitGroup
}

func (c *Cache) init() {
	c.once.Do(func() {
		c.group = single.NewGroup[[]byte]()
		if c.Now == nil {
			c.Now = time.Now
		}
		if c.Rand == nil {
			var mu sync.Mutex
			r := rand.New(rand.NewSource(time.Now().UnixNano()))
			c.Rand = func() *rand.Rand {
				// math/rand's Rand is not safe for concurrent use, and the
				// race detector says so under any real load.
				mu.Lock()
				defer mu.Unlock()
				return rand.New(rand.NewSource(r.Int63()))
			}
		}
		if c.StaleFor == 0 {
			c.StaleFor = DefaultStaleFor
		}
		if c.Log == nil {
			c.Log = slog.New(slog.NewTextHandler(discard{}, nil))
		}
	})
}

type discard struct{}

func (discard) Write(p []byte) (int, error) { return len(p), nil }

func (c *Cache) Stats() *Stats { c.init(); return &c.stats }

// Wait blocks until background refreshes have finished. For the tests and
// for a clean shutdown.
func (c *Cache) Wait() { c.init(); c.refresh.Wait() }

// Get returns the value for a key, computing it if it has to.
func (c *Cache) Get(ctx context.Context, key string) ([]byte, error) {
	c.init()
	now := c.Now()

	raw, found, err := c.Store.Get(ctx, key)
	if err != nil {
		// A cache that cannot be read is a cache miss, not a failed
		// request. Redis being down should make this slow, not broken.
		c.Log.Warn("cache read failed", "key", key, "err", err)
		found = false
	}

	if found {
		entry, derr := Decode(raw)
		switch {
		case derr != nil:
			// A value this process cannot read is not a value. Treating it
			// as a miss recovers automatically from an old encoding, which
			// is otherwise a manual flush during a deploy.
			c.Log.Warn("discarding an unreadable cache entry", "key", key, "err", derr)

		case !entry.Stale(now):
			c.stats.Hits.Add(1)
			if ShouldRecompute(entry, now, c.Rand()) {
				c.stats.EarlyRefresh.Add(1)
				c.refreshInBackground(key)
			}
			return entry.Value, nil

		default:
			// Past its deadline but still present. Refresh it, and serve
			// the old value meanwhile rather than making this caller wait.
			c.stats.Misses.Add(1)
			c.stats.ServedStale.Add(1)
			c.refreshInBackground(key)
			return entry.Value, nil
		}
	}

	c.stats.Misses.Add(1)
	return c.fill(ctx, key)
}

// fill computes the value, collapsing concurrent callers onto one call.
func (c *Cache) fill(ctx context.Context, key string) ([]byte, error) {
	res := c.group.Do(ctx, key, func(ctx context.Context) ([]byte, error) {
		start := c.Now()
		c.stats.OriginCalls.Add(1)

		value, err := c.Origin(ctx, key)
		if err != nil {
			c.stats.OriginErrors.Add(1)
			return nil, err
		}

		entry := Entry{
			Value:     value,
			WrittenAt: c.Now(),
			TTL:       Jitter(c.TTL, c.Rand()),
			Delta:     c.Now().Sub(start),
		}
		encoded, err := entry.Encode()
		if err != nil {
			return value, nil // the value is good even if caching it is not
		}
		// The physical lifetime is the logical one plus the stale window,
		// or a stale value could never be served because the store would
		// already have dropped it.
		if err := c.Store.Set(ctx, key, encoded, entry.TTL+c.StaleFor); err != nil {
			c.Log.Warn("cache write failed", "key", key, "err", err)
		}
		return value, nil
	})

	if res.Shared {
		c.stats.Collapsed.Add(1)
	}
	return res.Value, res.Err
}

// refreshInBackground recomputes a key without making anybody wait.
//
// The context is deliberately not the request's. The caller that triggered
// the refresh is about to be served from the old value and will go away,
// and cancelling the refresh with it would mean the key is never refreshed
// by anybody.
func (c *Cache) refreshInBackground(key string) {
	c.refresh.Add(1)
	go func() {
		defer c.refresh.Done()
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		if _, err := c.fill(ctx, key); err != nil && !errors.Is(err, context.Canceled) {
			// Not fatal: the stale value is still being served, which is
			// the point of keeping it.
			c.Log.Warn("background refresh failed, still serving stale", "key", key, "err", err)
		}
	}()
}

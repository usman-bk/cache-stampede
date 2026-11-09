package cachet

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
)

// Memory is an in process Store, for tests and local runs.
type Memory struct {
	mu    sync.RWMutex
	items map[string]memEntry
	now   func() time.Time
}

type memEntry struct {
	value   []byte
	expires time.Time
}

func NewMemory() *Memory {
	return &Memory{items: map[string]memEntry{}, now: time.Now}
}

func (m *Memory) SetClock(f func() time.Time) { m.mu.Lock(); m.now = f; m.mu.Unlock() }

func (m *Memory) Get(ctx context.Context, key string) ([]byte, bool, error) {
	if err := ctx.Err(); err != nil {
		return nil, false, err
	}
	m.mu.RLock()
	e, ok := m.items[key]
	now := m.now()
	m.mu.RUnlock()

	if !ok || now.After(e.expires) {
		return nil, false, nil
	}
	out := make([]byte, len(e.value))
	copy(out, e.value)
	return out, true, nil
}

func (m *Memory) Set(ctx context.Context, key string, value []byte, ttl time.Duration) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	cp := make([]byte, len(value))
	copy(cp, value)
	m.mu.Lock()
	defer m.mu.Unlock()
	m.items[key] = memEntry{value: cp, expires: m.now().Add(ttl)}
	return nil
}

func (m *Memory) Close() error { return nil }

func (m *Memory) Len() int { m.mu.RLock(); defer m.mu.RUnlock(); return len(m.items) }

// Redis is the Store this runs on.
type Redis struct{ client *redis.Client }

func NewRedis(addr string) (*Redis, error) {
	opts, err := redis.ParseURL(addr)
	if err != nil {
		opts = &redis.Options{Addr: addr}
	}
	// A cache that takes half a second to answer is worse than no cache:
	// the request pays the wait and then does the work anyway.
	opts.DialTimeout = 500 * time.Millisecond
	opts.ReadTimeout = 150 * time.Millisecond
	opts.WriteTimeout = 150 * time.Millisecond
	return &Redis{client: redis.NewClient(opts)}, nil
}

func (r *Redis) Ping(ctx context.Context) error { return r.client.Ping(ctx).Err() }

func (r *Redis) Get(ctx context.Context, key string) ([]byte, bool, error) {
	b, err := r.client.Get(ctx, key).Bytes()
	if errors.Is(err, redis.Nil) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, fmt.Errorf("cache get: %w", err)
	}
	return b, true, nil
}

func (r *Redis) Set(ctx context.Context, key string, value []byte, ttl time.Duration) error {
	if err := r.client.Set(ctx, key, value, ttl).Err(); err != nil {
		return fmt.Errorf("cache set: %w", err)
	}
	return nil
}

func (r *Redis) Close() error { return r.client.Close() }

var (
	_ Store = (*Memory)(nil)
	_ Store = (*Redis)(nil)
)

// Package single collapses concurrent work on the same key.
//
// The failure it exists for: a popular key expires, two thousand requests
// miss within the same few milliseconds, and every one of them goes to the
// database. The database was sized for the cache hit rate it normally
// enjoys, so it falls over, and when it recovers the key is still cold and
// it happens again.
//
// The cache was not the problem. The cache working perfectly and then
// stopping for one instant was the problem.
package single

import (
	"context"
	"sync"
)

// Result is one computed value, shared by everybody who waited for it.
type Result[T any] struct {
	Value T
	Err   error
	// Shared reports whether this caller did the work or waited for
	// somebody else's. It is on the result rather than hidden, because it
	// is the number worth putting on a dashboard: a collapse rate near zero
	// means this package is doing nothing.
	Shared bool
}

type call[T any] struct {
	wg      sync.WaitGroup
	value   T
	err     error
	waiters int
}

// Group runs one function per key at a time, in this process.
//
// In process, not across the fleet. Ten replicas means up to ten calls to
// the origin rather than two thousand, and that is enough: the point is to
// turn a thundering herd into a trickle, not to elect a single leader. A
// distributed lock would cut it to one and costs a network round trip on
// every miss, a lock to expire correctly when a holder dies, and a new
// failure mode where the lock service is down. See the README.
type Group[T any] struct {
	mu    sync.Mutex
	calls map[string]*call[T]

	// stats
	total     int64
	collapsed int64
}

func NewGroup[T any]() *Group[T] {
	return &Group[T]{calls: map[string]*call[T]{}}
}

// Do runs fn for key, or waits for the call already running.
//
// A caller that arrives while fn is in flight waits for its result rather
// than starting a second one. That is the whole mechanism.
func (g *Group[T]) Do(ctx context.Context, key string, fn func(context.Context) (T, error)) Result[T] {
	g.mu.Lock()
	g.total++
	if c, running := g.calls[key]; running {
		c.waiters++
		g.collapsed++
		g.mu.Unlock()

		// Waiting on the WaitGroup rather than on ctx would mean a caller
		// that gave up still blocks until the origin answers. A request
		// whose client has hung up should stop.
		done := make(chan struct{})
		go func() { c.wg.Wait(); close(done) }()
		select {
		case <-done:
			return Result[T]{Value: c.value, Err: c.err, Shared: true}
		case <-ctx.Done():
			var zero T
			return Result[T]{Value: zero, Err: ctx.Err(), Shared: true}
		}
	}

	c := new(call[T])
	c.wg.Add(1)
	g.calls[key] = c
	g.mu.Unlock()

	// The context here is the first caller's, which is a trap worth naming:
	// if that caller goes away the work is cancelled for everybody waiting
	// on it. Callers that care pass a context detached from the request.
	c.value, c.err = fn(ctx)

	g.mu.Lock()
	delete(g.calls, key)
	g.mu.Unlock()
	c.wg.Done()

	return Result[T]{Value: c.value, Err: c.err}
}

// Stats reports how many calls arrived and how many were collapsed onto
// somebody else's work.
func (g *Group[T]) Stats() (total, collapsed int64) {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.total, g.collapsed
}

// InFlight is how many keys are being computed right now.
func (g *Group[T]) InFlight() int {
	g.mu.Lock()
	defer g.mu.Unlock()
	return len(g.calls)
}

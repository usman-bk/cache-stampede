package single_test

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/usmanvirk/cache-stampede/internal/single"
)

func TestOneCallForManyCallers(t *testing.T) {
	g := single.NewGroup[string]()
	var calls atomic.Int64

	const n = 500
	var wg sync.WaitGroup
	start := make(chan struct{})
	results := make(chan single.Result[string], n)

	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			results <- g.Do(context.Background(), "k", func(context.Context) (string, error) {
				calls.Add(1)
				time.Sleep(20 * time.Millisecond)
				return "value", nil
			})
		}()
	}
	close(start)
	wg.Wait()
	close(results)

	if got := calls.Load(); got != 1 {
		t.Fatalf("%d callers produced %d calls, want 1", n, got)
	}
	shared := 0
	for r := range results {
		if r.Err != nil {
			t.Fatal(r.Err)
		}
		if r.Value != "value" {
			t.Fatalf("a caller got %q", r.Value)
		}
		if r.Shared {
			shared++
		}
	}
	if shared != n-1 {
		t.Fatalf("%d callers reported sharing, want %d", shared, n-1)
	}
}

func TestDifferentKeysRunConcurrently(t *testing.T) {
	g := single.NewGroup[int]()
	var calls atomic.Int64

	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			g.Do(context.Background(), fmt.Sprintf("k%d", i), func(context.Context) (int, error) {
				calls.Add(1)
				time.Sleep(10 * time.Millisecond)
				return i, nil
			})
		}(i)
	}
	wg.Wait()

	if got := calls.Load(); got != 20 {
		t.Fatalf("20 keys produced %d calls, want 20", got)
	}
}

func TestASecondCallAfterTheFirstFinishesRunsAgain(t *testing.T) {
	g := single.NewGroup[int]()
	var calls atomic.Int64
	fn := func(context.Context) (int, error) { calls.Add(1); return 1, nil }

	g.Do(context.Background(), "k", fn)
	g.Do(context.Background(), "k", fn)

	// Collapsing is not caching. Once the work is done the key is no
	// longer in flight, and the next caller does it again.
	if got := calls.Load(); got != 2 {
		t.Fatalf("%d calls, want 2: this collapses concurrent work, it does not remember results", got)
	}
}

func TestTheErrorReachesEverybody(t *testing.T) {
	g := single.NewGroup[int]()
	boom := errors.New("origin is down")

	var wg sync.WaitGroup
	start := make(chan struct{})
	errs := make(chan error, 50)
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			r := g.Do(context.Background(), "k", func(context.Context) (int, error) {
				time.Sleep(10 * time.Millisecond)
				return 0, boom
			})
			errs <- r.Err
		}()
	}
	close(start)
	wg.Wait()
	close(errs)

	for err := range errs {
		if !errors.Is(err, boom) {
			t.Fatalf("a waiter got %v, want the error the call produced", err)
		}
	}
}

// TestAWaiterThatGivesUpDoesNotBlock. A request whose client has hung up
// should stop, not wait for an origin it no longer needs.
func TestAWaiterThatGivesUpDoesNotBlock(t *testing.T) {
	g := single.NewGroup[int]()
	release := make(chan struct{})

	go func() {
		g.Do(context.Background(), "k", func(context.Context) (int, error) {
			<-release
			return 1, nil
		})
	}()

	// Let the first caller take the key.
	deadline := time.Now().Add(2 * time.Second)
	for g.InFlight() == 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if g.InFlight() == 0 {
		t.Fatal("the first call never started")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()

	start := time.Now()
	r := g.Do(ctx, "k", func(context.Context) (int, error) { return 2, nil })
	elapsed := time.Since(start)

	if r.Err == nil {
		t.Fatal("a waiter whose context expired got a value")
	}
	if elapsed > time.Second {
		t.Fatalf("the waiter blocked for %s after its context expired", elapsed)
	}
	close(release)
}

func TestStats(t *testing.T) {
	g := single.NewGroup[int]()
	var wg sync.WaitGroup
	start := make(chan struct{})
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			g.Do(context.Background(), "k", func(context.Context) (int, error) {
				time.Sleep(20 * time.Millisecond)
				return 1, nil
			})
		}()
	}
	close(start)
	wg.Wait()

	total, collapsed := g.Stats()
	if total != 10 {
		t.Fatalf("total is %d, want 10", total)
	}
	if collapsed != 9 {
		t.Fatalf("collapsed is %d, want 9", collapsed)
	}
	if g.InFlight() != 0 {
		t.Fatalf("%d keys still in flight after everybody finished", g.InFlight())
	}
}

// Package cachet is a read-through cache that does not fall over when a
// popular key expires.
//
// Three things cause the fall, and they need three different answers.
//
//  1. Many callers miss the same key at once. Answered by collapsing them
//     onto one call to the origin.
//  2. Many keys expire at the same moment, because they were written at the
//     same moment with the same TTL. Answered by spreading the expiry.
//  3. The origin is slow or down, so every miss waits for a timeout and
//     then fails. Answered by serving the stale value.
//
// Doing only the first is the common mistake. It turns two thousand
// simultaneous calls into one, and does nothing about two thousand keys all
// expiring on the same second after a deploy warmed them together.
package cachet

import (
	"encoding/json"
	"fmt"
	"math"
	"math/rand"
	"time"
)

// Entry is what is stored: the value, when it was written, and how long it
// was meant to live.
//
// The deadline is stored in the value rather than left to the store's own
// expiry, because the whole scheme depends on reading a value that is past
// its logical deadline and deciding what to do with it. A key the store has
// already deleted cannot be served stale.
type Entry struct {
	Value []byte `json:"v"`
	// WrittenAt and TTL define the logical deadline. After it the value is
	// stale but still present.
	WrittenAt time.Time     `json:"w"`
	TTL       time.Duration `json:"t"`
	// Delta is how long the origin took to produce this value, which is
	// what the early recompute is scaled by: an expensive value is worth
	// starting earlier.
	Delta time.Duration `json:"d"`
}

func (e Entry) ExpiresAt() time.Time { return e.WrittenAt.Add(e.TTL) }

// Stale reports whether the logical deadline has passed.
func (e Entry) Stale(now time.Time) bool { return now.After(e.ExpiresAt()) }

// Age is how long since it was written.
func (e Entry) Age(now time.Time) time.Duration { return now.Sub(e.WrittenAt) }

func (e Entry) Encode() ([]byte, error) { return json.Marshal(e) }

func Decode(raw []byte) (Entry, error) {
	var e Entry
	if err := json.Unmarshal(raw, &e); err != nil {
		return Entry{}, fmt.Errorf("decoding cache entry: %w", err)
	}
	if e.TTL <= 0 {
		return Entry{}, fmt.Errorf("cache entry has a ttl of %s", e.TTL)
	}
	return e, nil
}

// Beta controls how eagerly a value is recomputed before it expires.
//
// This is probabilistic early expiration, from Vattani, Chierichetti and
// Lowenstein's "Optimal Probabilistic Cache Stampede Prevention" (2015).
// A caller recomputes early with a probability that rises as the deadline
// approaches, scaled by how long the value took to produce last time.
//
// The appeal is that it needs no coordination at all. Every caller decides
// on its own, using only what is in the entry, and the result is that one
// of them refreshes the key before it ever goes cold, while the rest keep
// getting hits.
//
// 1.0 is the value the paper recommends. Above it, recomputation starts
// earlier and costs more origin calls; below it, the value more often goes
// genuinely cold.
const Beta = 1.0

// ShouldRecompute decides whether this caller refreshes a value that is
// still valid.
//
//	now - delta * beta * ln(rand) >= expiry
//
// ln of a uniform random number is negative, so the left side is the
// current time pushed forward by a random multiple of how expensive the
// value was. Far from the deadline the probability is tiny; close to it,
// it approaches certainty.
func ShouldRecompute(e Entry, now time.Time, rnd *rand.Rand) bool {
	if e.Delta <= 0 {
		// Nothing is known about the cost, so there is nothing to scale by
		// and the value is simply used until it expires.
		return false
	}
	r := rnd.Float64()
	if r <= 0 {
		r = math.SmallestNonzeroFloat64
	}
	early := time.Duration(float64(e.Delta) * Beta * -math.Log(r))
	return now.Add(early).After(e.ExpiresAt())
}

// Jitter spreads a TTL so that keys written together do not expire
// together.
//
// The case this is for: a deploy, or a cache flush, warms several thousand
// keys within a second of each other, all with a 300 second TTL. Five
// minutes later every one of them expires in the same second. Collapsing
// concurrent callers does not help, because these are different keys.
//
// Plus or minus 10%, which is enough to spread a thousand keys over a
// minute and small enough that the TTL still means roughly what it says.
func Jitter(ttl time.Duration, rnd *rand.Rand) time.Duration {
	if ttl <= 0 {
		return ttl
	}
	spread := float64(ttl) * 0.2
	return ttl - time.Duration(spread/2) + time.Duration(rnd.Float64()*spread)
}

# cache-stampede

_Built with AI assistance._

A read-through cache that does not take the database down when a popular
key expires.

    2000 callers, 1 key, a 50ms origin, a 2s ttl, through redis

    run          origin calls  hits  collapsed  served stale  wall time
    no defences  2000          0     0          0             113ms
    collapsed    1             0     1999       0             56ms

Go 1.23, Redis.

```
internal/single/    collapsing concurrent callers onto one call
internal/cachet/    the entry, the early refresh, the jitter, the cache
cmd/stampede/       the table above
```

## Three failures, three answers

The cache was not the problem. The cache working perfectly and then
stopping for one instant was the problem. There are three ways that
happens and they need different fixes, which is why doing only the first
one is the common mistake.

**Many callers miss the same key at once.** A popular key expires, two
thousand requests miss within a few milliseconds, and all of them go to a
database sized for the hit rate it normally enjoys. Answered by collapsing
them: the first caller does the work, the rest wait for its answer.

**Many keys expire at the same moment.** A deploy warms three thousand keys
within a second, all with a 300 second TTL, and five minutes later they all
expire together. Collapsing does nothing here, because these are different
keys. Answered by jittering the TTL by plus or minus 10%, which spreads a
thousand keys across a minute while leaving the TTL meaning roughly what it
says.

**The origin is slow or down.** Every miss waits for a timeout and then
fails. Answered by keeping the value past its logical deadline and serving
it while a refresh runs behind.

## The value expires twice

An entry carries its own deadline, and the store is given a longer one.

    written ──────── logical deadline ──────── physical expiry
            fresh                  stale, still served
                                   └── StaleFor, 10 minutes by default

Past the logical deadline the value is stale: it is returned immediately and
a refresh starts in the background. Past the physical expiry it is gone and
a caller waits.

Leaving expiry to the store does not work, because a key the store has
already deleted cannot be served stale. That is the whole reason the
deadline is inside the value.

The stale window is a promise about how wrong an answer may be. Ten minutes
is a judgement: long enough to cover an outage somebody can respond to,
short enough that nothing is served from a value nobody would recognise. It
is a field rather than a constant because the right answer for a price is
not the right answer for a product description.

## Refreshing before it goes cold

Even with all of the above, the first caller after the deadline waits for
nothing and gets a stale value, and the key spends some time being stale.
Probabilistic early expiration removes even that.

    now - delta * beta * ln(rand) >= expiry

From Vattani, Chierichetti and Lowenstein, *Optimal Probabilistic Cache
Stampede Prevention* (2015). `delta` is how long the origin took last time,
so an expensive value starts refreshing earlier. `ln` of a uniform random
number is negative, so the left side is the clock pushed forward by a random
multiple of that cost: far from the deadline the chance is vanishing, close
to it, near certain.

What makes it worth having is that it needs no coordination at all. Each
caller decides alone from what is already in the entry, and one of them
refreshes the key before anybody meets it cold.

**It only helps where the origin is slow.** If `delta` is zero the scheme
switches itself off, which is correct and worth knowing: there is nothing to
gain by refreshing a free value early. That was found by a test, where a
frozen test clock made every origin call appear instant and the early
refresh never fired. The fix was in the test, and the behaviour is now
documented rather than surprising.

## Collapsing is in process, not across the fleet

Ten replicas means up to ten calls to the origin rather than two thousand.
That is enough.

A distributed lock would cut it to one, and costs a network round trip on
every miss, a lock that has to expire correctly when its holder dies, and a
new dependency that can be down. The point is to turn a herd into a trickle,
not to elect a leader, and ten calls to a database is not a problem.

Collapsing is also not caching. Once the work finishes the key is no longer
in flight and the next caller does it again; the cache is what remembers.
A case in `cache_test.go` pins that, because the two are easy to confuse.

A waiter whose context is cancelled stops waiting. The alternative is that a
request whose client hung up still blocks until the origin answers.

## The one trap in it

The function that does the work runs under the **first** caller's context.
If that caller goes away, the work is cancelled for everybody waiting on it.
A background refresh therefore uses a context detached from any request,
because the caller that triggered it is about to be served a stale value and
leave.

## Nothing here fails a request

Every cache error is logged and treated as a miss. Redis being down makes
this slow, not broken. An entry that cannot be decoded is also a miss, which
means a change to the stored shape recovers by itself instead of needing a
manual flush during a deploy.

## Running it

    REDIS_URL=redis://localhost:6379 go run ./cmd/stampede
    go run ./cmd/stampede -callers 5000 -keys 20 -origin-ms 200

With no `REDIS_URL` it uses an in process store, which still shows the
collapse because the collapse is in process anyway.

## Tests

    go test ./...                   20 cases in process
    REDIS_URL=... go test ./...     24, adding the Redis suite

The interesting ones are the failure cases: two thousand callers producing
one origin call, a stale value served while the origin is down, the error
surfacing once the stale window has passed, and a thousand jittered TTLs
landing on more than fifty distinct seconds rather than one.

## Limits

No negative caching. A key that does not exist is a miss every time, which
is its own stampede and wants a different answer, because caching an absence
and getting it wrong is worse than caching a value and getting it wrong.

No per key configuration. One TTL and one stale window for everything, where
a real system wants them per kind of value.

No way to invalidate a key deliberately. Everything here is time based, and
a write that should drop a cached value immediately has nowhere to say so.

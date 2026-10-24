# ADR-0008: Token bucket rate limiting evaluated in Redis

**Status**: Accepted
**Date**: 2026-10-26

## Context

The API is multi-tenant and shared, so one tenant submitting a large batch must
not consume the capacity that other tenants' notifications need. Limits are
per tenant, defaulting to `RATE_LIMIT_PER_MINUTE` with a per-tenant override
stored on the tenant row.

Two things about the placement of the limiter matter:

1. **Where the counter lives.** The API is designed to run as multiple processes
   (ADR-0011), so an in-process counter would effectively multiply the limit by
   the number of processes. The counter has to be shared.
2. **What happens when the counter is unavailable.** If the limiter's store is
   down, the choice is between rejecting traffic ("fail closed") and admitting it
   ("fail open").

## Decision Drivers

- **Must** be shared across API processes and, at this scale, be consistent
  enough that a tenant cannot meaningfully exceed its limit by racing requests.
- **Must** be cheap. This runs on every authenticated request, including the
  authenticated read endpoints.
- **Must** fail in the direction that does not drop customer notifications.
- **Should** allow a deliberate burst, because real clients submit in batches
  rather than at a constant rate.

## Considered Options

### Option 1: Fixed-window counter (`INCR` + `EXPIRE`)

- **Pros**: One Redis round trip, trivial to reason about.
- **Cons**: Bursting at a window boundary allows up to 2x the limit (send the
  full allowance at t=59s and again at t=61s). The metric then disagrees with
  what tenants actually experience.

### Option 2: Token bucket, read-modify-write from the application

- **Pros**: Smooth, allows bursts, no boundary artefact.
- **Cons**: Two or more Redis calls per request, and a race between concurrent
  requests that both read the same token count and both decrement it, admitting
  more than the limit.

### Option 3: Token bucket as a Redis Lua script

- **Pros**: Smooth, burst-tolerant, and atomic — Redis executes the script
  without interleaving, so the read, refill, decrement and write cannot race.
  One round trip.
- **Cons**: The bucket logic lives in Lua, which is less familiar and cannot be
  unit-tested as plain Python.

### Option 4: Limit at the gateway or proxy

- **Pros**: Off the application path entirely.
- **Cons**: The limit is per tenant, which requires the gateway to authenticate
  and resolve tenants — duplicating the API's auth logic in the proxy, and
  making the effective limit invisible to the application.

## Decision

We will evaluate a **token bucket as a Redis Lua script**, keyed per tenant,
called from the authentication dependency so every authenticated endpoint is
limited by construction. On a Redis error the limiter **fails open**, admitting
the request and incrementing `ntf_ratelimit_backend_errors_total`.

## Rationale

The decisive driver was consistency at the boundary. A fixed window lets a
tenant double its intended rate through timing alone, which makes the limit
advisory rather than real, and makes capacity planning wrong in a way that only
appears under exactly the burst conditions the limit exists to handle.

Atomicity is why the bucket lives in Lua rather than in Python. A read-modify-
write from the application has a genuine race: concurrent requests can each read
the same token balance and each decrement it. At 120 req/s measured against the
single-tenant path, and with several API processes intended, that race is not
hypothetical.

Fail-open follows from what this limiter protects against. A rate limiter exists
to keep one tenant from crowding out others; a limiter store outage is a far
smaller problem than rejecting notifications that the system has promised to
deliver. Availability of *delivery* outranks enforcement of *fairness*, so the
failure direction is explicit rather than accidental.

The limiter lives in the auth dependency specifically so that a new endpoint
cannot be added without one — the constraint is structural rather than
documented.

## Consequences

### Positive

- Smooth limiting with an intentional burst allowance, no boundary artefact.
- One Redis round trip; measured at **0.76 ms** median, comparable to the tenant
  cache read at 0.62 ms.
- Atomic under concurrency, so the limit holds under simultaneous requests.
- Verified twice: a burst against a 5/min tenant admitted exactly the 60-token
  burst and rejected the rest (`{202: 60, 429: 20}`); and a 100-burst test
  admitted 72 against a 600/min default, the extra being refill during the loop.
- A rate-limiter outage degrades fairness, never availability.

### Negative

- The bucket logic is Lua, so it cannot be unit-tested directly; it is tested
  through observable behaviour (`tests/test_ratelimit.py`) instead.
- The limiter costs a Redis round trip on every authenticated request, including
  reads that would otherwise be served entirely from cache.
- A client that exceeds the limit is rejected *after* authentication, so
  rejected requests still cost an API-key hash and a Redis lookup.

### Risks

- **Fail-open is exploitable during a Redis outage.** Accepted deliberately: the
  alternative drops legitimate notifications.
- **A per-tenant override is cached alongside the tenant**, so changing the limit
  takes up to `TENANT_CACHE_TTL_SECONDS` to take effect. Documented rather than
  worked around, because the alternative is a database read per request.
- **Bucket keys and the work queue share a Redis instance**, so an eviction
  policy that removes `rl:*` keys would reset limits mid-window. Same
  operational note as ADR-0002.

## Implementation Notes

- `app/ratelimit.py`: `TOKEN_BUCKET` Lua script; `check_rate_limit` returns
  `(allowed, remaining, retry_after_seconds)`.
- The script is registered with `register_script`, giving `EVALSHA` with an
  automatic `NOSCRIPT` retry after a Redis restart.
- `app/routes.py`, `current_tenant`: sets `X-RateLimit-Limit` and
  `X-RateLimit-Remaining` on success, and **repeats them inside the
  `HTTPException`** on rejection. An `HTTPException` builds its own response, so
  headers set on the injected `Response` are discarded — a client being
  throttled is precisely the one that needs to see the limit.
- Verified `429` response headers: `retry-after: 12`, `x-ratelimit-limit: 5`,
  `x-ratelimit-remaining: 0`.
- Capacity is `RATE_LIMIT_BURST`, refill is `limit_per_minute / 60` per second,
  and the key TTL is the time to refill an empty bucket.

## Related Decisions

- ADR-0011: multiple API processes are the reason the counter is shared.

## References

- `app/ratelimit.py`, `app/routes.py`, `tests/test_ratelimit.py`
- README, *API*.

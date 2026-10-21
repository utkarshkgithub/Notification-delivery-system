# Notification Delivery Infrastructure

A multi-tenant notification delivery service. Accept a notification over REST,
the service persists it, queues it, delivers it to the right channel, retries
with exponential backoff, dead-letters what cannot be delivered, and keeps a
queryable audit trail of every state transition.

Built as a backend portfolio project against a Python/Django/FastAPI + Postgres +
Redis + ClickHouse job description. Everything below runs with one command and
every performance number was measured on this code.

```
                                    +------------------+
  POST /v1/notifications            |     FastAPI      |      202 Accepted
  -------------------------->  ---> |  auth + limits   | ---> immediately, before
                                    |  store + enqueue |      any delivery happens
                                    +---------+--------+
                                              |
                     +------------------------+------------------------+
                     |                                                 |
              +------v------+                                   +------v------+
              |  Postgres   |  current state, source of truth   |    Redis     |
              |             |  (status, attempts, errors)       |  - queue     |
              +------+------+                                   |  - rate limit|
                     |                                          |  - cache     |
                     |  read/write                              +------+-------+
                     |                                                 |
                     |                                          +------v-------+
                     |                                          | Celery worker|
                     |                                          |  (8 threads) |
                     |                                          +------+-------+
                     |                                                 |
                     |                    +----------------------------+---------------------+
                     |                    |               |                               |
                     |              email sender    webhook sender             in_app sender
                     |                                    |  HMAC-signed
                     |                                    v
              +------v-------+                    customer's endpoint
              |  ClickHouse  |  append-only event history, analytics only,
              |              |  never in the delivery path
              +--------------+
                     ^
              +------+-------+
              |  Prometheus  |  scrapes /metrics from API and worker
              +--------------+
```

## Why this shape

| Choice | Reason |
| --- | --- |
| **Postgres is the source of truth** | Notification state is relational and transactional: one row, updated as the delivery progresses. Every API read comes from here. |
| **Redis is the queue, not the record** | Celery + Redis gives at-least-once delivery and scheduled retries without the operational weight of Kafka. If Redis is flushed, accepted notifications are still in Postgres and the sweeper re-publishes them. |
| **ClickHouse is a separate store** | Attempt history grows without bound; the delivery tables must stay small and indexable. Analytics queries (`p95 webhook latency this hour`) are a bad fit for the row store and a good fit for a column store. |
| **Analytics can never fail a delivery** | The event sink is best effort and off the request path. A ClickHouse outage degrades analytics, not delivery, and shows up as `503` on the analytics endpoint rather than as an empty result that looks like real data. |
| **`acks_late` + idempotent delivery** | A worker that dies mid-delivery must not lose the notification. Redelivery is safe because the worker checks the row's state before sending. |

## Quickstart

```bash
cp .env.example .env
docker compose up -d --build          # api, worker, beat, postgres, redis, clickhouse, prometheus

# create a tenant - the API key is printed once, only its hash is stored
docker compose exec api python -m app.cli create-tenant acme
```

Then send a notification:

```bash
curl -sS -X POST http://localhost:8000/v1/notifications \
  -H 'Authorization: Bearer ntf_...' \
  -H 'Content-Type: application/json' \
  -d '{"channel":"webhook","recipient":"https://example.com/hook","body":"hello"}'
```

Bundled services: API docs at <http://localhost:8000/docs>, Prometheus at
<http://localhost:9090>, and a demo webhook receiver on
<http://localhost:8090/_stats> that verifies signatures the way a real customer
would.

## API

| Method | Path | Notes |
| --- | --- | --- |
| `POST` | `/v1/notifications` | `202` + the row. `Idempotency-Key` replays the original instead of creating a second notification (`200` on replay). |
| `GET` | `/v1/notifications/{id}` | Cache-aside read; the worker invalidates on every state change. |
| `GET` | `/v1/notifications` | Keyset (cursor) pagination, filterable by `status` and `channel`. |
| `POST` | `/v1/notifications/{id}/retry` | `202` for a dead-lettered notification, `409` for anything not dead. |
| `GET` | `/v1/stats` | Current-state counts by status, from Postgres. |
| `GET` | `/v1/stats/events` | Per-event history from ClickHouse. `503` when the store is degraded. |
| `GET` | `/healthz` | Liveness plus Postgres and Redis. ClickHouse is deliberately excluded so an analytics outage cannot pull the API out of a load balancer. |
| `GET` | `/metrics` | Prometheus exposition. |

Errors follow HTTP semantics: `401` bad key, `404` unknown or another tenant's
notification, `409` invalid state transition, `422` validation, `429` over the
tenant's rate limit with `Retry-After` and the `X-RateLimit-*` headers.

## Delivery lifecycle

```
                    +--------+
   accepted  -----> | queued | --------------------------+
                    +--------+                           |
                        ^                                | worker picks it up
                        |                                v
                        |                          +-------------+
        2xx from the    |                          | delivering  |
        endpoint        |                          +------+------+
                        |                                 |
                        |              +------------------+------------------+
                        |              |                  |                  |
                        |        2xx: delivered     4xx: permanent     5xx/timeout:
                        |                             no retry          retry with
                        |                                                 backoff
                        |                                                  |
                        |                        +-------------------------+
                        |                        |
                        |                 attempts < max?
                        |                        |
                        |              yes: back to queued (scheduled), attempts++
                        |              no:  +------+  manual POST .../retry
                        |                   | dead |  ---------------->
                        |                   +------+
                        +-------------------------+
```

- **Backoff** is exponential with jitter, capped, and the retry is *scheduled*
  on the broker rather than `sleep`-ing inside a worker thread.
- **4xx is permanent.** A `400` or `401` from a customer endpoint will never
  succeed, so it dead-letters immediately instead of burning the retry budget.
- **Dead letter is a state, not a separate queue.** Dead rows are still in
  Postgres, queryable and retryable through the API.
- **Two safety nets for lost work.** `reclaim_stuck_deliveries` re-queues rows
  left in `delivering` by a worker that died (found by `status, next_attempt_at`).
  `sweep_queued_notifications` re-publishes rows that were accepted but whose
  broker message vanished. Both skip work that is already scheduled or in flight,
  so neither turns into a duplicate-delivery storm.

## Data model

`tenants`, `notifications`, `delivery_attempts`. Three indexes do all the
work, all three verified with `EXPLAIN (ANALYZE)` rather than assumed:

| Index | Serves | Measured plan |
| --- | --- | --- |
| `ix_notifications_tenant_created (tenant_id, created_at, id)` | list + stats, keyset pagination | `Index Scan Backward`, 51 rows, **0.128 ms** |
| `ix_notifications_due (status, next_attempt_at)` | reclaimer / sweeper | `Index Scan`, `Index Cond` on both columns, **0.053 ms** |
| `uq_notifications_tenant_idem (tenant_id, idempotency_key)` | idempotent create, and the race that two concurrent identical requests cause | unique constraint, `IntegrityError` handled as "return the winner" |

Pagination is keyset, not `OFFSET`: page 1000 is the same single index seek as
page 1. The tenant's rate limit and the API key hash are cached in Redis so a
cache hit authenticates without touching Postgres at all.

## Observability

Prometheus counters and histograms for the things that page you: notifications
accepted, delivery attempts by outcome, retry/duration histograms, dead-lettered,
stuck reclaimed, sweep requeued, `429`s, rate-limiter backend failures, cache
hit/miss, ClickHouse write failures, and HTTP latency by route.

Labels are deliberately bounded (`channel`, `outcome`, `method`, `route`,
`status`) and never `tenant_id` or `notification_id` — unbounded label values
turn a metric into a memory leak and a scrape into a timeout.

`docker compose up` starts Prometheus already scraping the API and the worker.

## Performance

All numbers below are from this repository on an 8-core Linux box, with every
container (API, worker, beat, Postgres, Redis, ClickHouse, Prometheus) sharing
those cores. Reproduce with `scripts/loadtest.py`.

### The one that mattered: prefetch starvation — 51x

With Celery's `worker_prefetch_multiplier=1` and `acks_late`, the worker holds
roughly one message in flight per thread. The pool then sits idle waiting for
the broker while a task takes milliseconds of work. Measured back-to-back on a
drained queue, 1000 `in_app` notifications at concurrency 16:

| Arm | Ingest | Drain | End to end |
| --- | --- | --- | --- |
| `WORKER_PREFETCH_MULTIPLIER=1` | 110.8 req/s | **191.0 s** (5.2 deliveries/s) | 200.1 s (5.0/s) |
| `WORKER_PREFETCH_MULTIPLIER=8` | 111.9 req/s | **3.7 s** (266.7 deliveries/s) | 12.7 s (78.8/s) |

Ingest is identical in both arms, so the difference is entirely dispatch. The
worker's CPU was near idle in both runs — it was never the work, it was the
waiting. Raising the multiplier only widens the window in which a crashed worker
has unacknowledged messages, and a redelivery of those is harmless here because
the worker skips anything already in flight or finished.

Reproduce both arms: `WORKER_PREFETCH_MULTIPLIER=1 docker compose up -d worker`
(see `app/tasks.py` for the reasoning in place).

### End to end, including real HMAC-signed webhooks

2000 notifications, concurrency 16, channel `webhook`, delivered to the bundled
receiver which verifies every signature:

```
accepted            2000/2000 (0 errors)
ingest throughput   78.7 req/s
ingest latency      p50 194ms  p95 302ms  p99 398ms
drain after ingest  11.2s (178.1 deliveries/s)
END TO END          36.6s (54.6 notifications/s)
receiver            2000 received, 2000 verified_signature, 2000 accepted
```

### Ingest

| Scenario | Throughput |
| --- | --- |
| API alone (worker paused, concurrency 64) | ~120 req/s, p50 520 ms |
| Whole stack running and draining, 6000 requests | 70.8 req/s sustained, **0 errors, no 5xx** |

The gap is CPU: with the stack running, ClickHouse and the worker are consuming
cores that the API would otherwise use. `scripts/loadtest.py` is a synchronous
`httpx` client and its own Python process competes for the same cores, so treat
these as system-level numbers, not an isolated API benchmark.

**Overload behaviour is honest, not graceful.** At 200 concurrent clients the
single-process API reaches p50 2.5 s and a small fraction of connections reset
instead of returning a clean `503`. Load shedding (`uvicorn --limit-concurrency`)
and more workers are the fix; see *Scaling* below.

### ClickHouse

`async_insert=1` merges single-row inserts into shared parts server-side —
58,946 rows landed in **5 parts**, so there is no "too many parts" problem from
per-event inserts. But `wait_for_async_insert=1` makes the client sit on the
server's buffer flush:

| Write | Median |
| --- | --- |
| plain insert (one part per row) | 4.8 ms |
| `async_insert=1, wait_for_async_insert=1` | **60.9 ms** (up to 220 ms under load) |
| `async_insert=1, wait_for_async_insert=0` | **2.6 ms** |
| `SELECT 1` (round-trip floor) | 1.1 ms |

Since the sink is best effort, waiting buys nothing and costs a lot, so this
project fires and forgets and keeps the server-side batching.

## Bugs found and fixed while building this

These are the parts worth talking about in an interview. Each was found by
measuring, not by reading.

**1. A single bad insert silently disabled analytics for 30 seconds.**
`clickhouse_connect` client was a module global, but it holds one session and
raises when two threads use it at once — and the worker runs threads. Under
load, concurrent use failed constantly, and my circuit breaker amplified the
first failure into a 30-second blackout. Result: **431 of 7727 events survived
(94% lost)** while all deliveries succeeded, which is exactly the failure mode a
best-effort sink is supposed to avoid. Fixed with thread-local clients, a real
failure threshold (`FAILURES_BEFORE_COOLDOWN`) so a single hiccup cannot black
out analytics, and a lock so the failure counter is not itself racy. After:
**100% capture** (4244 events for 2122 notifications) with
`ntf_clickhouse_write_errors_total 0`.

**2. Analytics was in the request path, costing 218 ms per request.**
`create_notification` recorded the `queued` event synchronously before
responding. Timing each component of the ingest path showed ClickHouse at
**217.7 ms median** against sub-millisecond Redis, which capped accept at ~65
req/s. Moved to a FastAPI `BackgroundTask` and stopped waiting for the flush.
Analytics is now a second datastore that cannot slow the API down.

**3. The `X-RateLimit-*` headers vanished exactly when a client needed them.**
They were set on the injected `Response`, and an `HTTPException` builds its own
response — so the `429` carried `Retry-After` but not the limit. Found by
actually reading a `429` response instead of trusting the happy path.

**4. The benchmark flattered itself.** `/v1/stats` is tenant-wide, so on a
second run against the same tenant the *previous* run's rows satisfied the drain
check instantly — the tool reported a 0.007 s drain. It now takes a baseline
before ingesting and measures a delta. A load generator you cannot trust is
worse than none, because it turns into a number on a resume.

Also: dropped redundant single-column indexes in favour of one composite (the
planner was ignoring the extras), and moved the worker from the default
`prefork` pool to `threads` so `/metrics` reports one coherent registry instead
of forking delivery counters into children that are never scraped.

## Tests

```bash
docker compose run --rm -v "$PWD":/srv api pytest -q     # 75 passed
```

The suite runs against **real Postgres and Redis**, not mocks of the pipeline.
Channel senders are stubbed at the network edge so the retry ladder, dead-letter
path, reclaimer and request-scoped caches are exercised for real:

- `test_api.py` — accept, idempotency (including the concurrent race), pagination
  without gaps or repeats, filters, stats, tenant isolation.
- `test_delivery.py` — success, 5xx retry with backoff, permanent 4xx, exhaustion
  to dead, manual retry, cache invalidation, stuck delivery reclaimed, queued
  sweeper including the notifications it must *not* touch.
- `test_ratelimit.py` — bucket capacity, the headers on `429`, rejected requests
  not stored, per-tenant override, fail-open when Redis is down.
- `test_signing.py` — signature verification and replay-window rejection.
- `test_events.py` — the sink never breaks delivery, and a read never reports
  "store is down" as "no data".

## Configuration

See `.env.example` for the full list with comments. The compose defaults tighten
the delivery timers (1 s retry base, 20 s stuck timeout, 15 s sweep grace) so the
failure paths are visible in a demo instead of hours away; production values are
in the same file.

## Scaling

The API is stateless and the worker scales out by adding replicas; both only
touch shared state (Postgres, Redis, ClickHouse). Two things to know before
raising concurrency:

- **`/metrics` is process-local.** Extra uvicorn workers (or Celery `prefork`
  children) each keep their own registry, so a scrape sees one process and the
  counters look like they only count a fraction of the traffic. Scale with
  `prometheus_client` multiprocess mode, or scrape per-replica. This is why the
  committed default is `UVICORN_WORKERS=1` and the worker uses the `threads`
  pool.
- **Connection math.** The SQLAlchemy pool is `pool_size=10, max_overflow=20`
  per process. Four API workers is 120 connections against Postgres' default
  `max_connections=100`, so the pool must be scaled down as workers go up.

## Known limitations

- At-least-once, not exactly-once: a redelivery can send the same webhook twice.
  Deduplication is the receiver's job via `X-Notification-Id`.
- The `email` channel uses a console transport unless SMTP is configured; there
  is no provider integration.
- ClickHouse is single-node with a 90-day TTL and no sharding.
- Under sustained overload the reclaimer can re-publish faster than work
  completes, which amplifies a backlog. It is safe (delivery is idempotent) but
  the timeout should be well above the real p99 delivery time.

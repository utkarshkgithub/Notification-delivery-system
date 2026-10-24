# ADR-0011: One uvicorn process, and the metrics consequence

**Status**: Accepted
**Date**: 2026-10-26

## Context

Ingest throughput was measured at several concurrency levels with the worker
paused, so that the API was not competing with delivery or analytics:

| Concurrency | Throughput | p50 latency |
| --- | --- | --- |
| 16 | 99.0 req/s | 152 ms |
| 32 | 111.5 req/s | 273 ms |
| 64 | 119.9 req/s | 520 ms |

Throughput is essentially flat past concurrency 32 while latency doubles, which
is the signature of a saturated single process rather than a saturated host: the
API is CPU-bound on one core doing Pydantic validation, SQLAlchemy work, JSON,
and Celery serialisation. With the whole stack running and draining, sustained
throughput was 70.8 req/s over 6,000 requests with **0 errors**.

At 200 concurrent clients the picture degrades honestly: p50 2.5 s, p99 11 s, and
3 of 4,000 connections reset mid-response (a stale keep-alive reuse under
saturation, not corruption). No 5xx and no data loss, but the overload behaviour
is "slow and a few resets", not "shed load cleanly with a 503".

The obvious response is `uvicorn --workers N`. Two constraints make that a
decision rather than a default:

1. **`/metrics` is process-local.** Prometheus counters live in the process that
   increments them. With N workers, each keeps its own registry and a scrape
   hits exactly one of them, so the reported rate is roughly `1/N` of the truth.
   This is the same failure mode as the Celery `prefork` pool (ADR-0004), and it
   is the dangerous kind: the dashboard looks healthy while throughput is wrong.
2. **Connection arithmetic.** The SQLAlchemy engine is
   `pool_size=10, max_overflow=20` — 30 connections per process. Four workers is
   120 connections against Postgres' default `max_connections=100`, so the pool
   must be scaled down as workers go up, or Postgres starts refusing connections.

## Decision Drivers

- **Must not** report metrics that are a fraction of reality.
- **Must** keep the scaling path available and documented, not discovered in an
  incident.
- **Should** keep the default configuration honest rather than maximally fast.
- **Should** keep `/metrics` a single scrape per service.

## Considered Options

### Option 1: One uvicorn process (chosen default)

- **Pros**: `/metrics` is complete and truthful. No pool arithmetic to get wrong.
  Simplest to reason about. ~120 req/s is above the throughput this system
  targets.
- **Cons**: One CPU-bound process is the ceiling. No redundancy: a crash takes
  the API down until restart (mitigated by `restart: unless-stopped`).

### Option 2: Several workers with Prometheus multiprocess mode

- **Pros**: Uses the other cores; metrics remain correct.
- **Cons**: Requires `PROMETHEUS_MULTIPROC_DIR` on a shared volume plus
  aggregation at scrape time, and the pool must be divided by the worker count.
  Real machinery for a load this system does not currently have.

### Option 3: Several API replicas, each scraped individually

- **Pros**: Scales across hosts, not just cores; each container keeps an honest
  registry; failures are isolated.
- **Cons**: Needs a load balancer and a per-replica scrape (or aggregation in
  Prometheus). The right answer for horizontal scale, and deliberately deferred
  until there is traffic that needs it.

### Option 4: Make the endpoint `async def` and tune the DB layer

- **Pros**: Could reduce per-request cost.
- **Cons**: The work is genuinely synchronous (SQLAlchemy, `redis-py`, Celery),
  so an `async def` handler would block the event loop rather than help. A
  separate, larger question about the data layer, out of scope for the process
  model.

## Decision

We will run the API as **one uvicorn process by default**, exposed as
`UVICORN_WORKERS` so it is a supported knob rather than a hardcoded value,
and documented together with the two constraints above. Throughput is raised by
adding **replicas** (option 3) before adding workers within a container.

## Rationale

The decisive driver was not shipping a dashboard that lies. Raising worker count
is a one-line change that appears to improve capacity while silently dividing
every metric by N — the failure mode would surface as "traffic dropped 75%"
during an incident when the real cause was a configuration change. Refusing that
as a default is a deliberate trade of peak throughput for observability, and it
is the same reasoning already applied to the Celery pool in ADR-0004, so the
system has one consistent convention instead of two.

Replicas are preferred over in-container workers because they keep the one-scrape-
per-process property for free, isolate failures, and scale past one machine's
cores — which workers never can.

## Consequences

### Positive

- `/metrics` from the API is complete; every counter describes all traffic.
- Committed defaults need no pool arithmetic to be correct.
- The scaling path is written down with its constraints, including the specific
  pool maths, so raising concurrency is a considered change rather than a guess.
- Verified: 6,000 requests at concurrency 64 sustained 70.8 req/s with 0 errors
  and no 5xx.

### Negative

- Roughly 120 req/s is the practical ceiling for one API container, achieved
  only when the rest of the stack is idle. Under full stack load it is ~70 req/s.
- No process redundancy: a hard crash interrupts service until the container
  restarts.
- Overload is not graceful. At 200 concurrency the process slows and resets a
  few connections instead of returning `503`, so a client with no retry logic
  sees an error without a `Retry-After` signal.

### Risks

- **The ingest ceiling is reached before the system is unhealthy elsewhere.**
  The worker can drain at 266 deliveries/s (ADR-0004) while the API ingests at
  ~120 req/s, so under a large burst the API saturates first and the queue grows.
  This is visible in `/v1/stats` (`queued` climbing) and is the metric to alert
  on.
- **A future change sets `UVICORN_WORKERS` without reading this.** Mitigated by
  the comment at the compose command and in `.env.example`, both of which point
  here.
- **Keep-alive resets under saturation** could be mistaken for an application
  bug. Recorded here so the symptom is recognisable.

## Implementation Notes

- `docker-compose.yml`: the api command takes `--workers ${UVICORN_WORKERS:-1}`.
  `UVICORN_WORKERS` was documented in `.env.example` before it was actually
  wired up; it is now passed through, so the knob does what the docs say.
- Pool settings live in `app/db.py`: `pool_size=10, max_overflow=20,
  pool_recycle=1800`. Divide by the worker count when raising it.
- Load shedding is the next step for the overload case:
  `uvicorn --limit-concurrency` converts resets into explicit `503`s.
- Measure with the worker paused to get the API's own capacity; measure with the
  full stack to get system throughput. The two numbers differ by ~2x on an 8-core
  box and both are honest for their question.

## Related Decisions

- ADR-0004: the same per-process metrics constraint, on the worker side.
- ADR-0008: the shared rate-limit counter that multiple processes require.

## References

- `docker-compose.yml`, `app/db.py`, `.env.example`
- README, *Performance → Ingest* and *Scaling*.

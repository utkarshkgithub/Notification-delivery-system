# ADR-0002: Celery on Redis as the delivery queue

**Status**: Accepted
**Date**: 2026-10-26

## Context

Accepting a notification and delivering it are deliberately separated: the API
must answer in milliseconds, while delivery is a slow, failure-prone call to a
third party. Something has to hold work between the two, survive a worker
restart, and support "try again in 30 seconds" without holding a thread open.

The scale to design for is modest and known: a few hundred notifications per
second at peak, retries measured in single-digit percentages, and a delivery
budget of "the user should not notice" rather than a hard real-time bound.

The team already runs Redis in this stack for caching and rate limiting, so the
real question is whether a queue needs a separate broker.

## Decision Drivers

- **Must** persist accepted work across a worker or broker restart. A
  notification that returned `202` must not silently vanish.
- **Must** support delayed execution for the retry ladder, without blocking a
  worker thread for the delay.
- **Should** be operable by one team without a dedicated platform function.
- **Should not** put a second, unfamiliar piece of infrastructure in the
  critical path if the existing one will do.

## Considered Options

### Option 1: Celery with Redis as broker and result backend

- **Pros**: Redis is already a dependency, so no new operational surface.
  Celery has first-class delayed tasks (`countdown`), retries, scheduled beat,
  and at-least-once acknowledgement (`acks_late`). Well understood by Python
  backend teams, which matters for hiring and handover.
- **Cons**: Redis is memory-bound and not durable by default, so a queue built
  on it can lose messages. Redis is not designed as a message broker: no
  consumer groups, no real replay.
- **Measured**: dispatch throughput is sensitive to worker configuration — see
  ADR-0004, where prefetch tuning moved drain time by 51x.

### Option 2: Celery with RabbitMQ

- **Pros**: A real broker with durable queues, proper acknowledgements and
  per-message TTL. Better default durability than Redis.
- **Cons**: A second data service to run, monitor, back up and secure. The
  feature that actually matters here (durable delayed delivery) has to be
  emulated anyway, since per-message TTL plus a dead-letter exchange is the
  usual workaround rather than a queue feature.

### Option 3: Kafka

- **Pros**: Durable log, replay, high throughput, natural fit for the event
  stream if history had stayed in the system.
- **Cons**: Delivery here is *one task per notification*, not a stream — Kafka
  is a poor fit for per-item delaying with per-item retry budgets. Consumer
  group rebalancing makes "retry this one message in 30 seconds" awkward.
  Operationally heavy for a few hundred messages per second, and it would not
  remove the need for the ClickHouse event store that already covers history.

### Option 4: Postgres as the queue (`SELECT ... FOR UPDATE SKIP LOCKED`)

- **Pros**: No new technology at all. Transactional with the state row, so
  accepting and enqueueing are atomic.
- **Cons**: Polling or `LISTEN/NOTIFY` for wakeup, and queue throughput becomes
  competing load on the database that serves the API. Roll-your-own retry and
  visibility-timeout logic, which is precisely where subtle bugs live.

## Decision

We will use **Celery backed by Redis** for delivery, with `acks_late` and
`task_reject_on_worker_lost` so a worker that dies mid-delivery has its message
redelivered, plus a periodic reclaimer and sweeper to cover the two ways work
can go missing (ADR-0005).

## Rationale

The decisive driver was that Redis is already a dependency, and the delivery
volume does not justify a second broker. Celery's `countdown` maps directly onto
the retry ladder, and its at-least-once semantics are the guarantee this system
wants rather than a workaround.

The known weakness — Redis can lose queued messages — is not ignored; it is
covered structurally. Every notification is committed to Postgres *before* it is
enqueued, so Postgres remains the authoritative record of accepted work, and the
sweeper re-publishes queued rows whose message is missing. This means the queue
is treated as an accelerator, never as the record, which is the property that
makes an in-memory broker acceptable here.

## Consequences

### Positive

- No new data service; the broker is the cache/limiter Redis.
- Delayed retries are native (`countdown`), so no thread sleeps.
- At-least-once is explicit and testable: `acks_late` plus idempotent delivery.
- Redis runs with AOF (`--appendonly yes`), so a restart does not drop the queue.

### Negative

- Redis persistence is weaker than a real broker's durability contract.
- Queue depth and Celery semantics are less observable than a broker with
  consumer groups; the queue is inspected via `LLEN` and metrics.
- Redis holds both transient cache data and the work queue. A `FLUSHALL` would
  take out the queue; recovery depends on the sweeper.

### Risks

- **A `FLUSHALL` or eviction drops queued work.** Accepted and mitigated: rows
  stay `queued` in Postgres and the sweeper re-publishes them. Note that
  cache-key eviction policy and queue keys share a Redis instance, so production
  should separate the queue into its own logical DB or instance.
- **Broker visibility timeout shorter than the largest retry delay** causes a
  task to be redelivered while its countdown has not elapsed. Mitigated:
  `visibility_timeout` (1800s) is set above the largest retry delay.

## Implementation Notes

- Celery configuration and the delivery task: `app/tasks.py`.
- Broker URL is the same `REDIS_URL` used by the cache and limiter.
- Enqueue happens **after** the Postgres commit — the reverse order would let a
  worker load a row that does not exist yet.
- `broker_transport_options={"visibility_timeout": 1800}` must stay above the
  largest retry delay.

## Related Decisions

- ADR-0004: how this broker behaves under worker configuration.
- ADR-0005: the guarantee this choice requires the rest of the system to provide.

## References

- `app/tasks.py`, `docker-compose.yml` (redis `--appendonly`)
- README, *Why this shape*, *Delivery lifecycle*.

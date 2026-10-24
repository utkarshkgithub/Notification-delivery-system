# ADR-0004: Worker pool: threads, with a tuned prefetch multiplier

**Status**: Accepted
**Date**: 2026-10-26

## Context

Two independent worker-level defaults turned out to be actively harmful on this
workload, and both were only found by measuring.

**Prefetch.** Celery's `worker_prefetch_multiplier` defaults to 4, and was set to
1 here on the reasoning that with `acks_late`, holding fewer unacknowledged
messages is safer. Combined with a Redis broker, that keeps roughly one message
in flight per worker thread. Delivery tasks take milliseconds, so the pool spent
almost all its time waiting on the broker rather than doing work. The worker sat
at **0.07% CPU** while a queue drained at 5.2 deliveries/s.

**Pool.** The default `prefork` pool forks worker processes. Prometheus
counters live in-process, so with `prefork` each child process has its own
registry, and a scrape of the parent reports a fraction of the real traffic.
This is not a cosmetic problem: it means the numbers on the dashboard that
would have shown the above are wrong, and wrong in the direction of "looks
fine".

Both were found by measuring rather than by reading configuration: extracting
per-task durations from the worker log showed a median task time of 33 ms while
the pool drained at 5.2/s, which is only possible if the worker is idle.

## Decision Drivers

- **Must** not idle the pool while work is available.
- **Must** not report misleading metrics. A dashboard that cannot show a
  throughput bug is worse than no dashboard.
- **Must** keep redelivery safe: raising prefetch widens the window in which a
  crashed worker holds unacknowledged messages.
- **Should** keep the configuration reproducible so the trade-off can be
  re-measured rather than taken on faith.

## Considered Options

### Option 1: `prefetch=1`, `prefork` pool (the starting point)

- **Pros**: Looks conservative. Fewest messages at risk on a crash.
- **Cons**: Measured 191.0 s to drain 1000 notifications (5.2 deliveries/s).
  Metrics report one process's counters only.

### Option 2: Default `prefetch=4`, `prefork` pool

- **Pros**: Better than 1 without going far.
- **Cons**: Still sized for CPU-bound tasks. Metrics problem remains.

### Option 3: `prefetch=8`, `threads` pool, `concurrency=8`

- **Pros**: Measured 3.7 s for the same 1000 notifications (266.7 deliveries/s).
  One process means one honest registry, so `/metrics` is complete. Delivery
  work is I/O-bound — Postgres and outbound HTTP plus ClickHouse writes — so the
  GIL is released for the majority of each task.
- **Cons**: A crashed worker can hold up to `prefetch x concurrency`
  unacknowledged messages. The `threads` pool gives up process-level isolation:
  a task that segfaults or leaks affects the whole worker.

### Option 4: `prefork` pool with Prometheus multiprocess mode

- **Pros**: Keeps process isolation and gets correct metrics.
- **Cons**: Requires `PROMETHEUS_MULTIPROC_DIR` with a shared volume and a
  scrape-time aggregation step. Real complexity, and it does not address
  prefetch at all.

## Decision

We will run the worker with **`--pool=threads --concurrency=8`** and a
**configurable `WORKER_PREFETCH_MULTIPLIER`, defaulting to 8**.

## Rationale

The decisive driver was that the workload is I/O-bound. Each delivery is
dominated by waiting: the Postgres state update, the outbound HTTP call, and the
ClickHouse event write all release the GIL. Threads therefore deliver the same
parallelism as processes here, without forking the metric registry — which
solves the observability problem and the throughput problem with one choice.

Prefetch is raised because the safety argument for 1 does not hold in this
system. A redelivered message is harmless: `attempt_delivery` skips anything
already in flight or already finished, so the cost of a wider redelivery window
is a wasted no-op rather than a duplicate delivery. Verified during the load
test: 2000 notifications delivered, all at exactly one attempt, with no
duplicates from reclaim or redelivery.

The multiplier is exposed as configuration rather than hardcoded so the A/B
comparison stays reproducible without editing code — which is the only reason
the 51x figure can still be checked.

## Consequences

### Positive

- Drain time for 1000 notifications: **191.0 s → 3.7 s (51x)**; end to end
  including ingest, **200.1 s → 12.7 s**.
- Ingest was identical in both arms (110.8 vs 111.9 req/s), which isolates the
  improvement to dispatch rather than to any change in the API.
- `/metrics` from the worker is complete, so the delivery counters are usable.

### Negative

- Losing process isolation: a hard crash in one task takes down all 8 threads.
  Mitigated by `task_reject_on_worker_lost`, which redelivers the in-flight
  messages, and by the reclaimer (ADR-0005).
- A crash now holds up to 64 messages unacknowledged rather than 8, so recovery
  is slightly bumpier.
- `threads` is the less-travelled Celery pool; a future Celery upgrade could
  change its behaviour.

### Risks

- **Prefetch 8 was tuned on an 8-core box with a single worker.** On different
  hardware the optimum may differ; the configuration is exposed precisely so it
  can be re-measured. Treat 8 as measured-but-not-universal, not as a constant.
- **Throughput measured on a host shared by seven other containers.** The 51x
  ratio comes from a back-to-back A/B on the same host and is a fair comparison;
  the absolute numbers are not an isolated benchmark.

## Implementation Notes

- `app/tasks.py`: `worker_prefetch_multiplier=settings.worker_prefetch_multiplier`.
- `docker-compose.yml`: worker command uses `--pool=threads --concurrency=8`, and
  passes `WORKER_PREFETCH_MULTIPLIER` through from the environment.
- Reproduce the A/B:
  `WORKER_PREFETCH_MULTIPLIER=1 docker compose up -d --force-recreate worker`,
  then drain a fixed batch; repeat with 8. Drain must start from an **empty**
  queue or the measurement includes the previous run's backlog.
- If `prefork` is ever needed (for CPU-heavy channels), Prometheus multiprocess
  mode becomes mandatory, not optional.

## Related Decisions

- ADR-0002: the broker whose semantics made prefetch 1 look safer than it is.
- ADR-0005: what makes a wider redelivery window acceptable.
- ADR-0011: the same metrics-per-process constraint at the API layer.

## References

- `app/tasks.py`, `docker-compose.yml`, `.env.example`
- README, *Performance → The one that mattered: prefetch starvation*.

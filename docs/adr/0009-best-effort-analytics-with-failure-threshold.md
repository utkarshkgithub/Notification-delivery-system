# ADR-0009: Best-effort analytics with a failure threshold

**Status**: Accepted
**Date**: 2026-10-26

## Context

Once ADR-0001 put event history in a second store and ADR-0003 took that store
off the request path, the remaining question is what happens **when ClickHouse
fails**. Two behaviours were needed, and they are not the same behaviour:

- **Writes must never affect delivery.** A notification delivery that succeeded
  must not be reported as failed because an analytics insert failed.
- **Reads must never lie.** A caller asking for event history must not receive an
  empty list when the truth is "the store is unreachable", because that answer is
  indistinguishable from "nothing happened" and will be believed.

The first implementation tried to satisfy the write case with an aggressive
circuit breaker: on any write error, disable the sink for 30 seconds. This was
wrong twice over.

**It was triggered by a bug, not by an outage.** The `clickhouse_connect` client
was a module-level global. That client holds a single session and raises if two
threads use it concurrently, and the worker runs threads (ADR-0004) — so
concurrent deliveries made inserts fail constantly, and the breaker turned one
failure into a 30-second blackout that dropped everything in between.
Measured: **431 of 7,727 events survived — 94% of the event history was silently
lost** while every notification still reported `delivered`.

**And its threshold was one.** Even without the bug, a single failed insert
disabling analytics for 30 seconds is the wrong shape of response: a transient
error under load is routine, and the cost of the breaker firing is far larger
than the cost of one missing event.

## Decision Drivers

- **Must** never cause, delay or fail a delivery.
- **Must** never report "no data" when the store is down.
- **Must not** let one transient error switch the whole sink off.
- **Should** surface the loss, so silent degradation is impossible.

## Considered Options

### Option 1: Retry the write, in the delivery path

- **Pros**: Fewer lost events.
- **Cons**: A failing analytics store becomes delivery latency, and a retry storm
  against a struggling store makes its recovery worse. Directly violates the
  first driver.

### Option 2: Swallow every error, keep retrying per event

- **Pros**: Maximum capture; simplest.
- **Cons**: A genuinely down store means every delivery pays a connection
  timeout. Worst case is worse than dropping the event.

### Option 3: Disable the sink on the first error (the initial implementation)

- **Pros**: Protects the delivery path hard.
- **Cons**: Measures wrong — 94% of events lost. Over-reacts to exactly the
  condition (concurrent load) under which you most want the data.

### Option 4: Per-thread clients, threshold breaker, separate read behaviour

- **Pros**: The concurrency bug disappears rather than being rate-limited. One
  hiccup is absorbed. Sustained failure still pauses the sink. Reads fail loudly.
- **Cons**: More state to get right — a failure counter that is itself shared
  across threads.

## Decision

We will treat the sink as follows:

- **Thread-local clients** (`threading.local()`), because the client is not
  thread-safe.
- **A failure threshold**: `FAILURES_BEFORE_COOLDOWN = 5` consecutive failures
  before the sink is paused, then a `COOLDOWN_SECONDS = 30` pause, with the
  client dropped so the next attempt starts clean.
- **Consecutive** failures: any success resets the counter, so intermittent
  errors never accumulate into a pause.
- **Writes swallow** their errors and increment
  `ntf_clickhouse_write_errors_total`; they never raise to the caller.
- **Reads raise** `ClickHouseUnavailable`, which `GET /v1/stats/events` maps to
  `503`.

## Rationale

The decisive driver was that the two directions have opposite correct answers.
For writes, the system's priority is that delivery is unaffected, so swallowing
is right. For reads, the priority is that the answer is trustworthy, so
swallowing is actively harmful — an empty list is the one wrong answer that
looks right.

The threshold exists because the breaker's cost is asymmetric. Firing too early
loses data wholesale (measured: 94%); firing a few failures late costs at most a
handful of events. A threshold of 5 with a reset on success puts the error on
the cheap side. The counter is guarded by a lock because the worker is threaded —
an unguarded counter is exactly the kind of shared-state bug that produced the
original 94% loss.

Fixing the concurrency bug *and* keeping the breaker is deliberate
belt-and-braces: the thread-local client removes the spurious failures, and the
threshold means that if a new source of transient errors appears, the sink
degrades gradually instead of falling off a cliff.

## Consequences

### Positive

- Verified after the fix: **4,244 events for 2,122 notifications — exactly 2 per
  notification, 100% capture**, with `ntf_clickhouse_write_errors_total 0`.
- A tenant-scoped check confirmed the same at a smaller scale: 500 notifications
  → 500 `queued` events, 500 distinct notification IDs.
- ClickHouse being stopped entirely does not prevent a notification from
  delivering, and `/v1/stats` from Postgres keeps answering `200`.
- Reads fail loudly: verified `GET /v1/stats/events` returned **`503`** with an
  explicit `event store unavailable: ...` reason while ClickHouse was down, and
  recovered to `200` when it came back.
- Loss is observable rather than silent.

### Negative

- Flush-time failures are not observed by the writer once
  `wait_for_async_insert=0` is in place (ADR-0003), so the write-error counter
  under-reports in that one specific case.
- The failure counter is process-local, so with multiple worker processes each
  learns about the outage independently and the cooldown is per process.
- A paused sink means events are dropped, not buffered for later. There is no
  replay.

### Risks

- **A permanently broken sink loses all history silently.** Mitigated by
  `ntf_clickhouse_write_errors_total` and by the loud read failure, but it is a
  real gap: the delivery path will never complain on its own.
- **A threshold tuned to the wrong load.** 5 was chosen against measured
  transient-error rates; a very different workload might want a different value.
  It is a named constant precisely so it can be changed with the reasoning intact.

## Implementation Notes

- `app/events.py`: `_local`, `_schema_lock`, `_failure_lock`, `_note_failure`,
  `_note_success`, `_drop_client`, `FAILURES_BEFORE_COOLDOWN`.
- `record()` returns `None` on failure by design; it must never raise. If a new
  call site needs to know about a write, it needs a new function, not an
  exception out of `record()`.
- `main.py` calls `ensure_schema()` at startup and logs a warning rather than
  failing to boot — analytics being unavailable is not a reason to refuse
  traffic.
- Covered by `tests/test_events.py`, including that a disabled sink does not even
  attempt a connection, that failures are counted rather than raised, that the
  cooldown stops the storm once the threshold is crossed, and that the cooldown
  expires so the sink recovers.

## Related Decisions

- ADR-0001: why the sink exists and why it is derivative.
- ADR-0003: why its failures are now harder to observe.
- ADR-0004: the threads that made the shared client unsafe.
- ADR-0006: the same classify-don't-blindly-retry reasoning, applied elsewhere.

## References

- `app/events.py`, `tests/test_events.py`
- README, *Bugs found and fixed*, item 1.

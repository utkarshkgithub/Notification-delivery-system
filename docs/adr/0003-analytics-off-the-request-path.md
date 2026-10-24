# ADR-0003: Keep analytics off the request path

**Status**: Accepted
**Date**: 2026-10-26

## Context

ADR-0001 split state into Postgres and history into ClickHouse. The first
implementation recorded the `queued` event inside the accept handler, before the
response was returned:

```python
NOTIFICATIONS_CREATED.labels(...).inc()
events.record(event="queued", ...)      # synchronous HTTP call to ClickHouse
deliver_notification.apply_async(...)
return notification
```

This looked harmless — the sink swallows its own errors, so it can never fail a
request. What it can do is **add latency to every request**, and a sink that
cannot fail a request still gets to slow one down.

Timing each component of the ingest path separately showed where the time went:

| Component | Median |
| --- | --- |
| Redis tenant-cache read | 0.62 ms |
| Redis token-bucket check | 0.76 ms |
| Redis cache write | 1.00 ms |
| **ClickHouse `record()`** | **217.7 ms** |

The sink was not a rounding error; it was essentially the entire cost of
accepting a notification. Ingest throughput was capped at ~65 req/s with the
full stack running, and every `POST` was paying a round trip to a store whose
only purpose is to describe what happened later.

The `wait_for_async_insert=1` setting, added so that ClickHouse would merge
single-row inserts into shared parts instead of writing a part per row, turns
each one-row insert into a wait on the server's async flush buffer.

## Decision Drivers

- **Must** accept latency must not depend on the analytics store at all.
- **Must not** regress ClickHouse's storage behaviour into a small-parts problem
  (an unbounded number of tiny parts is how a column store falls over).
- **Should** keep delivery-event loss best-effort and observable rather than
  making it transactional.

## Considered Options

### Option 1: Keep the synchronous write, accept the latency

- **Pros**: Simplest code. An insert error is seen immediately on the request
  path, so the failure counter is accurate.
- **Cons**: ~218 ms added to every accept; ingest capped near 65 req/s.

### Option 2: Synchronous write, `wait_for_async_insert=0`

- **Pros**: Cheap — measured 2.6 ms versus 60.9 ms for `wait=1`. Keeps
  server-side batching.
- **Cons**: Still a network call to a second datastore inside the request path.
  The API's accept latency still depends on a store that is not needed to accept
  the work, so a ClickHouse network partition becomes API latency.

### Option 3: Write in the background, still waiting for the flush

- **Pros**: Response latency no longer includes the insert.
- **Cons**: The background task still occupies a worker thread for ~60–220 ms,
  which just moves the ceiling instead of removing it.

### Option 4: Fire-and-forget in a background task

- **Pros**: The response is returned without touching ClickHouse, and the
  background task releases its thread almost immediately.
- **Cons**: A failure during the server-side async flush is no longer visible to
  the writer, weakening the error signal that drives the cooldown logic.

## Decision

We will **record events from FastAPI background tasks** (`background.add_task`)
and **set `wait_for_async_insert=0`**, keeping `async_insert=1` so ClickHouse
still merges single-row inserts server-side.

## Rationale

The decisive driver was that accept latency must not depend on the analytics
store. A background task removes ClickHouse from the request path entirely,
which is the property ADR-0001 implied but the first implementation did not
deliver.

`wait_for_async_insert=0` follows from deciding what this store is: a
best-effort sink whose contract is that it can never break delivery. If it is
optional, then paying a full flush round trip per event buys nothing — the
durable record of what happened is the state table, not the event log.

Accepting a weaker write-side error signal is the cost, and it is bounded: only
failures during the *server-side flush* become invisible. Connection failures,
name resolution failures and HTTP errors still raise synchronously, so a
ClickHouse outage is still detected and still trips the cooldown (ADR-0009).
`ntf_clickhouse_write_errors_total` therefore undercounts in one specific case,
which is documented in the code rather than papered over.

## Consequences

### Positive

- The accept path no longer performs a cross-store network call.
- `record()` cost dropped from 60–220 ms to a measured **2.6 ms**.
- Batching is preserved: 58,946 event rows landed in **5 parts**.
- The API's availability story is simple — ClickHouse being down does not affect
  accepting or delivering notifications, verified by stopping ClickHouse and
  watching a notification deliver anyway.

### Negative

- Flush-time failures are not observed by the writer, so the write-error counter
  can under-report.
- Events can be lost if the process dies between the commit and the background
  task running. Accepted: the event row is analytics, the state row is truth.
- Two mechanisms now contribute to the accept path (the response and the
  background task), which is slightly harder to reason about than one.

### Risks

- **The background task's own work becomes the bottleneck under load.** It is
  ~2.6 ms now rather than ~218 ms, a ~80x reduction in thread occupancy, but it
  is not zero. Measured later: at 200 concurrent clients the API saturates and
  resets a few connections — see ADR-0011 for the scaling answer.
- **A future change re-adds a synchronous write.** Mitigated by the comment on
  the `add_task` call and by this record.

## Implementation Notes

- `app/routes.py`, `create_notification`: `background.add_task(events.record, ...)`.
  The `BackgroundTasks` parameter must stay on the sync endpoint signature.
- `app/events.py`, `record()`: `settings={"async_insert": 1,
  "wait_for_async_insert": 0}`. The comment there records why.
- Do not add other cross-store calls to the request path for the same reason.
- `ensure_schema()` is idempotent and guarded by a lock, so background tasks on
  many threads do not race to create the table.

## Related Decisions

- ADR-0001: established that events are derivative; this record makes the
  consequences real.
- ADR-0009: how the remaining failures are handled once the write is
  fire-and-forget.
- ADR-0011: the throughput ceiling that remains after this change.

## References

- `app/routes.py`, `app/events.py`
- README, *Bugs found and fixed*, item 2; *Performance → ClickHouse*.

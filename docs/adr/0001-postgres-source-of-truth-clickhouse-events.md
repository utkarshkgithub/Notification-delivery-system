# ADR-0001: Postgres as source of truth, ClickHouse for event history

**Status**: Accepted
**Date**: 2026-10-26

## Context

The service has two very different data shapes living in it, and they were
pulling the schema in opposite directions:

1. **Current state.** Each notification has exactly one current status, attempt
   count, last error, and delivery time. This is small, relational, updated in
   place, and read by the API on every request. Rows are read by `id` or listed
   by tenant.
2. **History.** Every attempt produces an event: queued, retried, delivered,
   dead. This is append-only, grows without bound (one notification produces two
   or more events), and is queried in aggregate — "what is the p95 webhook
   latency over the last hour", "how many retries did this tenant cause
   yesterday".

Putting both in one store means the history table is the big one, and every
index on the state table competes for space and vacuum time with rows nobody
reads individually. At 1000 notifications/hour with retries, the event table is
~40x the size of the state table within a month.

## Decision Drivers

- **Must** keep API reads on the current-state path fast and predictable, no
  matter how much history exists.
- **Must** keep the write path for state transactional: a notification moves
  through statuses exactly once, in order.
- **Should** support aggregate analytics queries (counts, averages, percentiles
  by window) without those queries slowing down the API.
- **Should not** add an operational dependency that can take down delivery.

## Considered Options

### Option 1: Everything in Postgres, one schema

- **Pros**: One store, one backup story, transactional consistency between state
  and events, no new technology.
- **Cons**: The events table becomes the dominant table; aggregate scans compete
  with API traffic for the same buffer cache; retention means mass `DELETE`s and
  the vacuum pressure that comes with them.

### Option 2: Everything in Postgres, partitioned events table

- **Pros**: Still one store; `DROP PARTITION` is a cheap retention story.
- **Cons**: Solves retention and nothing else. Aggregate queries still scan the
  same rows the API is trying to serve, and Postgres is not a column store — a
  `GROUP BY` over millions of event rows is the wrong shape of work for it.

### Option 3: Postgres for state, ClickHouse for events

- **Pros**: Each store does what it is good at. State rows stay few and
  indexable forever. Event aggregation is a column-store scan and is fast.
  ClickHouse TTL handles retention declaratively.
- **Cons**: Two stores, two backup/upgrade stories. No transaction spans both, so
  the event history can lag or lose entries relative to the state table.

## Decision

We will keep **current state in Postgres as the single source of truth**, and
write **append-only delivery events to ClickHouse** as a derivative store for
history and analytics.

## Rationale

The decisive driver was keeping the API's read path predictable as history
grows. The state table is bounded by the number of *live and recent*
notifications; the event table is bounded by nothing. Mixing them means the
API's performance degrades over time for a reason unrelated to its own traffic.

ClickHouse's TTL also replaces a retention chore: the events table drops data
older than 90 days with no batch job, no `DELETE`, and no vacuum.

Accepting a derivative store costs consistency, which this system already
tolerates by design — see ADR-0009. The state table is authoritative; a missing
event is an analytics gap, never a lost or mis-delivered notification.

## Consequences

### Positive

- API reads and delivery writes stay on small, well-indexed Postgres tables
  regardless of history volume.
- Aggregates over event history run on a store designed for scans.
- Retention is declarative (`TTL toDateTime(event_time) + INTERVAL 90 DAY`).
- Verifying the split: 58,946 event rows landed in **5 ClickHouse parts**, so
  per-event inserts did not cause a small-parts problem.

### Negative

- Two stores to run, back up and upgrade; the compose stack has one more
  container.
- No cross-store transaction. An event can be lost on crash after the state
  commit, and the two stores can be inconsistent.
- A reader must know which store answers which question: current status from
  `/v1/stats`, history from `/v1/stats/events`.

### Risks

- **Analytics drifts from truth.** Accepted: the state table is authoritative,
  and `ntf_clickhouse_write_errors_total` surfaces drops.
- **Analytics becomes load-bearing for delivery anyway.** This is exactly what
  happened, and needed ADR-0003 to fix.
- **An operator reads a stale aggregate as fact.** Mitigated by making the
  analytics endpoint fail loudly (`503`) rather than return a partial result
  that looks complete — see ADR-0009.

## Implementation Notes

- Postgres schema: `app/models.py`, migration
  `alembic/versions/2b6d6b2559d1_initial_schema.py`.
- ClickHouse table and DDL: `app/events.py` (`delivery_events`), keyed
  `ORDER BY (tenant_id, event_time, notification_id)` — tenant first, because
  every read is tenant-scoped.
- The ClickHouse sink is optional at runtime: `CLICKHOUSE_ENABLED=false` runs the
  whole delivery path with no analytics, which is also how it is tested.

## Related Decisions

- ADR-0003: the consequence of this split that was initially got wrong.
- ADR-0009: what "derivative, best effort" is required to mean in code.

## References

- `app/events.py`, `app/models.py`
- README, *Why this shape* and *Performance → ClickHouse*.

# Architecture Decision Records

This directory records the significant architectural decisions behind the
Notification Delivery Infrastructure: what was decided, what the alternatives
were, and what the decision costs. The goal is that someone joining the project
can understand **why** the system looks the way it does without reverse
engineering it from the code, and that future maintainers can tell an
intentional trade-off from an accident.

Each record is immutable once accepted. If a decision changes, write a new ADR
that supersedes the old one and update the status of the old one — the history
of a decision is itself useful information.

## Index

| ADR | Title | Status | Date |
| --- | --- | --- | --- |
| [0001](0001-postgres-source-of-truth-clickhouse-events.md) | Postgres as source of truth, ClickHouse for event history | Accepted | 2026-10-26 |
| [0002](0002-celery-on-redis-as-the-delivery-queue.md) | Celery on Redis as the delivery queue | Accepted | 2026-10-26 |
| [0003](0003-analytics-off-the-request-path.md) | Keep analytics off the request path | Accepted | 2026-10-26 |
| [0004](0004-worker-pool-threads-and-prefetch.md) | Worker pool: threads, with a tuned prefetch multiplier | Accepted | 2026-10-26 |
| [0005](0005-at-least-once-with-idempotent-delivery.md) | At-least-once delivery with idempotent handling | Accepted | 2026-10-26 |
| [0006](0006-retry-policy-and-failure-classification.md) | Retry policy: jittered backoff, permanent vs retryable failures | Accepted | 2026-10-26 |
| [0007](0007-keyset-pagination-over-offset.md) | Keyset pagination instead of OFFSET | Accepted | 2026-10-26 |
| [0008](0008-token-bucket-rate-limiting-in-redis.md) | Token bucket rate limiting evaluated in Redis | Accepted | 2026-10-26 |
| [0009](0009-best-effort-analytics-with-failure-threshold.md) | Best-effort analytics with a failure threshold | Accepted | 2026-10-26 |
| [0010](0010-varchar-check-instead-of-native-enums.md) | VARCHAR + CHECK instead of native Postgres enums | Accepted | 2026-10-26 |
| [0011](0011-single-uvicorn-process-and-process-local-metrics.md) | One uvicorn process, and the metrics consequence | Accepted | 2026-10-26 |
| [0012](0012-api-key-storage-sha256-not-a-password-kdf.md) | Store API key hashes with SHA-256, not a password KDF | Accepted | 2026-10-26 |
| [0013](0013-webhook-authentication-hmac-with-timestamp.md) | Authenticate webhooks with HMAC over timestamp and body | Accepted | 2026-10-26 |

### Reading order

If you are new to the project, read them in this order rather than by number:

1. [0001](0001-postgres-source-of-truth-clickhouse-events.md) — the store split,
   which everything else follows from.
2. [0002](0002-celery-on-redis-as-the-delivery-queue.md) — how work moves.
3. [0005](0005-at-least-once-with-idempotent-delivery.md) — the guarantee, and
   [0006](0006-retry-policy-and-failure-classification.md) — what happens to a
   failed attempt.
4. [0004](0004-worker-pool-threads-and-prefetch.md) — the 51x finding, and the
   cost of getting it wrong.
5. [0003](0003-analytics-off-the-request-path.md) and
   [0009](0009-best-effort-analytics-with-failure-threshold.md) — how the
   analytics store was kept out of the delivery path, and what happened when it
   was not.
6. The rest for the details: pagination, rate limiting, process model, schema
   representation, and the two credentials.

## Status vocabulary

```
Proposed ──> Accepted ──> Deprecated
    │            │
    │            └──────> Superseded by ADR-NNNN
    └──────> Rejected
```

| Status | Meaning |
| --- | --- |
| **Proposed** | Written, under discussion, not yet implemented. Default for anything new. |
| **Accepted** | Decided and implemented. |
| **Deprecated** | No longer relevant (the system moved on, or the constraint that motivated it is gone). |
| **Superseded** | Replaced by a later ADR, which is linked from both records. |
| **Rejected** | Considered and deliberately not done. Recorded because a rejected option that keeps getting re-proposed wastes time. |

## When to write one

| Write an ADR | Skip the ADR |
| --- | --- |
| Store or broker technology choice | Dependency patch upgrades |
| What is durable vs derived, and where | Bug fixes |
| Delivery guarantees and failure semantics | Implementation detail inside one function |
| API contract and pagination shape | Configuration value changes |
| Security and trust boundaries | Routine maintenance |
| Anything expensive to reverse | Refactors with no external effect |

The test used here: **if reversing it would take more than a day, or if a smart
newcomer would plausibly ask "why on earth is it done this way", it gets a
record.** Anything cheaper lives in a code comment next to the line it explains,
which is where a reader actually needs it.

## How to add one

1. Copy `template.md` to `NNNN-short-title-with-dashes.md` using the next free
   four-digit number. Numbers are never reused, even if a record is later
   rejected.
2. Fill in the template. Keep it short — one decision, one to two pages.
3. Set the status to `Proposed` unless it is already implemented and agreed.
4. Add the row to the index table above.
5. Review, then set the status to `Accepted` and merge.

To supersede an existing decision, add `## Supersedes` / `## Superseded by`
links and update both statuses rather than editing what the old record said.

## Conventions

- **One decision per record.** If the Consequences section needs an "and also",
  it is probably two ADRs.
- **Name the losers.** A record with no rejected options means either the
  decision was trivial or the options were not really considered.
- **Real numbers, or none.** Where a decision was driven by measurement, the
  measurement and the method are included. Where it was driven by judgement,
  say so — an invented benchmark is worse than "we accepted the uncertainty".
- **Accepted records are not edited**, except to correct a factual error or to
  update status. New information goes in a new record.
- Every performance figure in these records was measured on this codebase by
  `scripts/loadtest.py` and the probe scripts described in the main README; the
  method and its limits are documented there.

## References

- [Documenting Architecture Decisions — Michael Nygard](https://cognitect.com/blog/2011/11/15/documenting-architecture-decisions)
- [MADR template](https://adr.github.io/madr/)
- [adr.github.io](https://adr.github.io/)

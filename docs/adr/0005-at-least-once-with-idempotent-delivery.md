# ADR-0005: At-least-once delivery with idempotent handling

**Status**: Accepted
**Date**: 2026-10-26

## Context

A notification that has been accepted with `202` must eventually be attempted.
Between the `202` and the delivery there are several places where work can be
lost, and each needs a different answer:

1. **The broker message is never delivered.** Redis can be flushed, a key
   evicted, or a message dropped. The notification row still says `queued`, and
   nothing will ever pick it up.
2. **A worker dies mid-delivery.** With `acks_late`, the message is
   unacknowledged and the row is stuck in `delivering`. The broker may
   redeliver, but the row's state would make the new attempt think a delivery is
   in progress.
3. **The process crashes after the receiver accepted but before the state was
   committed.** The receiver got the notification; our row says otherwise.

Exactly-once delivery is not achievable across an untrusted third-party
endpoint without coordination the receiver does not offer. The honest choice is
which failure mode to accept, and then to make the consequences tolerable.

## Decision Drivers

- **Must** never lose an accepted notification silently. Every accepted
  notification must reach a terminal state, or be visibly `queued`/`delivering`
  with an operator able to see that.
- **Must** never send the same notification twice concurrently, which would let
  duplicate deliveries race each other.
- **Should** keep the recovery machinery cheap enough to run every minute.
- **Should not** require the receiver to implement anything new.

## Considered Options

### Option 1: At-most-once (`acks_early`, no redelivery)

- **Pros**: Simplest; duplicates impossible.
- **Cons**: Losing a notification on a worker crash is the default outcome, for
  a system whose whole purpose is delivery. Rejected.

### Option 2: At-least-once, no deduplication

- **Pros**: Nothing is lost; simple.
- **Cons**: The claims in the state table stop meaning anything — a row in
  `delivering` might be a live delivery or an abandoned one, and duplicate
  attempts could be in flight simultaneously.

### Option 3: At-least-once with a read-then-write guard

- **Pros**: Nothing is lost; simple to read; concurrent duplicates mostly
  prevented.
- **Cons**: Check-then-act is not atomic. Two messages for one notification can
  both read `queued` before either of them writes, and both send. The window is
  small, but a duplicated message is the *normal* way to arrive at this guard, so
  the window is the case that matters rather than an edge case.

### Option 4: At-least-once with an atomic claim, and two recovery tasks

- **Pros**: Nothing is lost; concurrent duplicates prevented by the database
  rather than by timing; recovery is explicit and observable.
- **Cons**: The receiver can still see the same notification twice sequentially.
  Requires the receiver to deduplicate for true exactly-once effects. The claim is
  a denser statement than a status check.

### Option 5: Transactional outbox in Postgres

- **Pros**: Accept and enqueue become atomic, so "accepted but never enqueued"
  cannot happen.
- **Cons**: Requires a relay process plus `SELECT ... FOR UPDATE SKIP LOCKED`
  polling; a real design, but the sweeper already covers the same failure with
  far less machinery at this scale.

## Decision

We will provide **at-least-once delivery** with:

- `task_acks_late=True` and `task_reject_on_worker_lost=True`, so a dead worker's
  message is redelivered;
- an **atomic claim before every attempt** — one conditional
  `UPDATE ... RETURNING` takes the attempt only if the row is not terminal, not
  already in flight, and still has attempts remaining, so of two messages for the
  same notification exactly one can proceed;
- `reclaim_stuck_deliveries` — rows in `delivering` older than
  `STUCK_DELIVERY_TIMEOUT_SECONDS` are reset to `queued` and re-published;
- `sweep_queued_notifications` — rows `queued` and past
  `QUEUED_SWEEP_GRACE_SECONDS` are re-published, covering a message that never
  arrived;
- a unique `(tenant_id, idempotency_key)` constraint so a client retry of the
  *request* cannot create a second notification.

## Rationale

The decisive driver was that losing an accepted notification is the one outcome
this system cannot have. Between at-most-once and at-least-once, only one of
them keeps the promise implied by `202`.

Deduplication is then pushed into the attempt itself rather than bolted on as a
separate store: the notification row is the only coordination point needed, so
"already done" and "already running" are one indexed read. This is why the
system does not need a distributed lock or a dedupe table — the state machine is
the lock.

That is only true if taking the attempt is atomic, and the first implementation
was not: it read the row, checked the status, and mutated afterwards. The check
and the mutation are where the guarantee has to live, so the attempt is now taken
with a single conditional `UPDATE ... RETURNING`. Postgres serialises concurrent
claims on the same row, so one of them matches and the other gets nothing back —
the guarantee comes from the database rather than from the code happening to run
fast enough.

Both recovery tasks are deliberately **idempotent and safe to re-run at any
time**. That property is what makes them safe to run on a timer without
coordination, and it is why over-firing is a performance question (see the
overload note below) rather than a correctness one.

## Consequences

### Positive

- No accepted notification is lost, including across a worker kill.
- Concurrent duplicate attempts are prevented by the pre-attempt state check.
- Recovery is observable: `ntf_stuck_deliveries_reclaimed_total` and
  `ntf_queued_sweep_requeued_total`.
- Verified: a row forced into `delivering` with no queued message was reclaimed
  (counter 0 → 1, log `reclaimed 1 stuck deliveries`) and then delivered on
  attempt 2.
- Verified: the manual `retry` endpoint re-queues a `dead` notification (`202`)
  and refuses one that is not dead (`409`).
- Verified: two claims on one row, forced to overlap by running them from two
  connections with the first held uncommitted, produce exactly one winner. The
  same test fails against a read-then-write claim, so it pins the property rather
  than the implementation.

### Negative

- **Sequential duplicates are possible and must be the receiver's problem.** If
  the send succeeds but the state commit fails, the next attempt sends again.
  Mitigated by publishing `X-Notification-Id` on every webhook so a receiver can
  deduplicate; documented rather than hidden.
- The status field now carries real meaning — `delivering` must only ever be set
  while an attempt is genuinely in flight, and the timing constants must be
  larger than a real delivery takes.

### Risks

- **Overload amplification.** Under sustained backlog, rows sit in `delivering`
  longer than `STUCK_DELIVERY_TIMEOUT_SECONDS`, so the reclaimer re-publishes
  work faster than it completes. Observed during benchmarking: 88 reclaim events
  and a queue of ~10k duplicate no-op messages, all of which drained harmlessly.
  It is safe because attempts are idempotent, but the timeout must be set
  comfortably above the real p99 delivery time — the compose default of 20 s is
  deliberately aggressive so failure paths are visible in a demo, and is **not**
  a production value.
- **A receiver that is slow rather than down** looks identical to a crashed
  worker. If p99 delivery time approaches the stuck timeout, the reclaimer will
  fight legitimate work.

## Implementation Notes

- `app/tasks.py`: `attempt_delivery` (state check, attempt, backoff, dead-letter),
  `reclaim_stuck_deliveries`, `sweep_queued_notifications`.
- `attempt_delivery` takes the attempt through `_claim_attempt`: one conditional
  `UPDATE ... RETURNING` requiring the status to be neither terminal nor
  `delivering`-and-fresh, and requiring attempts to remain. A `None` result means
  this message lost the claim, and `_unclaimable` then reads the row to explain
  why. No row lock is held across the network call — the claim commits before the
  send, so a webhook timeout cannot hold a lock for its duration.
- `_unclaimable` deliberately checks "in flight" **before** "attempts
  exhausted": a duplicate arriving while the final attempt is still running must
  return, not dead-letter a notification that is still being delivered.
- The sweeper **pushes `next_attempt_at` forward** when it re-publishes, so the
  same batch is not re-published every interval while it is in transit.
- Recovery tasks run under Celery beat; both take their intervals from settings.
- The reclaim path is covered by `tests/test_delivery.py`
  (`test_crashed_delivery_is_reclaimed`), including the case where it must
  **not** touch a healthy in-flight delivery.

## Related Decisions

- ADR-0002: the broker weakness this record compensates for.
- ADR-0004: the wider redelivery window this record makes acceptable.
- ADR-0006: what happens to a single attempt that fails.

## References

- `app/tasks.py`, `tests/test_delivery.py`
- README, *Delivery lifecycle* and *Known limitations*.

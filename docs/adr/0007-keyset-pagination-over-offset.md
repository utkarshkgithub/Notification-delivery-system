# ADR-0007: Keyset pagination instead of OFFSET

**Status**: Accepted
**Date**: 2026-10-26

## Context

`GET /v1/notifications` is a tenant-scoped, newest-first list. Tenants
accumulate notifications continuously, so the table only grows, and "the first
page" is always changing as new rows arrive.

There are two conventional ways to page a growing list, and they behave very
differently as the offset grows:

- `LIMIT n OFFSET k` — the database must produce and then discard `k` rows
  before returning anything. Cost grows linearly with page depth, so page 1000
  scans 50,000 rows to return 50.
- Keyset (cursor) pagination — the client sends the position of the last row it
  saw, and the query seeks directly to it via an index.

There is also a correctness problem specific to a live list: with `OFFSET`, rows
inserted or deleted between two requests shift every subsequent page, so a
client paginating while notifications arrive sees duplicates and skips rows.
Both symptoms are reported as bugs and are extremely hard to reproduce.

## Decision Drivers

- **Must** cost the same at page 1000 as at page 1.
- **Must** not skip or duplicate rows when the underlying data changes between
  requests.
- **Should** be expressible with a single composite index.
- **Should** keep the API contract simple for the client.

## Considered Options

### Option 1: `LIMIT` / `OFFSET`

- **Pros**: Trivial to implement and to consume; `page=1,2,3` is obvious to a
  client.
- **Cons**: Latency grows without bound with depth; rows shift under the client,
  producing skips and repeats.

### Option 2: Keyset pagination on a single column (`created_at`)

- **Pros**: Index seek, constant cost.
- **Cons**: `created_at` is not unique — notifications accepted in the same
  millisecond tie, and a strict `<` comparison then silently drops the tied rows.

### Option 3: Keyset pagination on `(created_at, id)`

- **Pros**: Constant cost via `(tenant_id, created_at, id)`; the tiebreaker makes
  the ordering total, so no row can be skipped or repeated.
- **Cons**: Opaque cursor for clients; no random access to "page 7"; jumps to a
  specific page require walking cursors.

## Decision

We will use **keyset pagination over the tuple `(created_at, id)`**, exposed as
an opaque base64 cursor:

```sql
WHERE tenant_id = :tenant
  AND (created_at, id) < (:last_created_at, :last_id)
ORDER BY created_at DESC, id DESC
LIMIT :limit + 1
```

The index `ix_notifications_tenant_created` is `(tenant_id, created_at, id)`.

## Rationale

The decisive driver was that the list only grows, so the cost of `OFFSET` is a
cost that is guaranteed to get worse. Deep-page latency is not a hypothetical
here; it is the steady state after a few months of traffic.

The `(created_at, id)` tuple exists specifically to make the ordering total.
With `created_at` alone, notifications accepted in the same millisecond — which
is the normal case under load, and was observed at 120 req/s during
benchmarking — would be dropped at the page boundary.

An `ORDER BY created_at DESC` over an ascending index is served by a backward
index scan, so the index does not need to be declared `DESC`.

## Consequences

### Positive

- Page 1 and page 1000 cost the same. Verified with `EXPLAIN (ANALYZE)`:
  `Index Scan Backward using ix_notifications_tenant_created`, 51 rows read,
  **0.128 ms** execution, 56 shared buffers.
- No skips or repeats while notifications are being written concurrently —
  covered by `test_list_paginates_without_gaps_or_repeats`.
- One index serves both the list and the stats query (its `(tenant_id,
  created_at)` prefix).

### Negative

- Clients cannot jump to an arbitrary page; the cursor is opaque and must be
  echoed back. This is a real API-shape cost, accepted rather than hidden.
- A cursor encodes a timestamp and an id, so it is not meaningful to cache or
  to construct by hand.
- The `LIMIT :limit + 1` trick to detect a next page means one row is fetched
  and discarded per page; negligible, but it is a deliberate choice rather than
  an accident.

### Risks

- **Cursor staleness.** A cursor points at a row that may later be deleted; the
  seek still works because it compares values rather than resolving the row.
  A malformed or foreign cursor is rejected as `400 invalid cursor` rather than
  trusted.

## Implementation Notes

- `app/routes.py`: `list_notifications`, `_encode_cursor`, `_decode_cursor`.
  The comparison uses SQLAlchemy's `tuple_()` so the row comparison is pushed to
  Postgres rather than evaluated in Python.
- Cursor format is `base64url({"ts": iso8601, "id": uuid})` — deliberately
  opaque, and versioned implicitly by the key names.
- `limit` is bounded `ge=1, le=200` at the edge so a client cannot request an
  unbounded page.
- Any future filter added to this endpoint must be index-compatible with the
  same ordering, or the constant-cost property is lost.

## Related Decisions

- ADR-0010: the column types and index shape this relies on.

## References

- `app/routes.py`, `tests/test_api.py`
- README, *Data model*.

# ADR-0010: VARCHAR + CHECK instead of native Postgres enums

**Status**: Accepted
**Date**: 2026-10-26

## Context

Several columns hold values from a small closed set: notification `status`
(`queued`, `delivering`, `delivered`, `dead`), `channel` (`in_app`, `webhook`,
`email`), attempt `outcome` (`delivered`, `failed`, `permanent`).

Postgres offers a native `ENUM` type for this, and SQLAlchemy will emit one by
default for a Python `Enum`. But the systems consuming these columns differ in
their tolerance for schema change:

- The API validates requests against the Python enum, so an unknown value is
  rejected at the edge.
- The worker writes values defined in code.
- ClickHouse mirrors `channel` and `event` as `LowCardinality(String)` — it has
  no knowledge of the Postgres enum type.
- The sets are expected to grow: new channels (SMS, push) and new outcomes are
  the obvious next features.

Adding a value to a native Postgres enum is `ALTER TYPE ... ADD VALUE`, which
before Postgres 12 could not run inside a transaction block at all, and which
still cannot be *removed* from an enum — there is no `DROP VALUE`. Alembic also
does not autogenerate these changes, so they must be hand-written and are easy
to get wrong.

## Decision Drivers

- **Must** keep adding a value a routine, reviewable migration.
- **Must** enforce validity at the database, not only in the application — the
  worker and any future process write to these columns too.
- **Should** keep the database readable when queried without the ORM.
- **Should** avoid a type whose changes Alembic cannot detect.

## Considered Options

### Option 1: Native Postgres `ENUM`

- **Pros**: Strongest typing; the database itself knows the value set; storage is
  slightly smaller.
- **Cons**: Adding a value is a special-case migration; values can never be
  removed; ordering is implicit by declaration order; Alembic will not
  autogenerate the change; the type must be created before the table and dropped
  after it in a migration, which is exactly the kind of ordering subtlety that
  breaks rollbacks.

### Option 2: `VARCHAR` with no constraint

- **Pros**: Trivial to change; nothing to migrate.
- **Cons**: Nothing stops a typo from writing `deliverd` into production, and the
  mistake surfaces as a notification that never appears in any query. Loses the
  main benefit of the enum type with none of the replacement's strengths.

### Option 3: `VARCHAR` with a `CHECK` constraint

- **Pros**: The database still rejects invalid values. Adding a value is a
  one-line `DROP CONSTRAINT` / `ADD CONSTRAINT` pair in a migration that Alembic
  and any reviewer can read. Values are stored as readable text.
- **Cons**: `CHECK` is not a real type, so nothing type-checks a comparison
  against the column, and the constraint must be updated by hand in the same
  migration that adds a value.

### Option 4: Integer codes with an application-level mapping

- **Pros**: Compact; renaming a value is free.
- **Cons**: The database becomes unreadable without a code table, and debugging
  `status = 3` in production is materially worse than reading `dead`.

## Decision

We will store these columns as **`VARCHAR(20)` with a `CHECK` constraint**,
configured in SQLAlchemy via:

```python
def _enum(enum_cls, name):
    return Enum(
        enum_cls,
        name=name,
        native_enum=False,          # VARCHAR + CHECK, no CREATE TYPE
        length=20,
        values_callable=lambda e: [m.value for m in e],
    )
```

## Rationale

The decisive driver was migration cost. The value sets in this project are
expected to grow, and a native enum turns "add a channel" into a special-case
migration that cannot be rolled back by removing the value. A `CHECK` constraint
is still enforced by the database but costs one readable line to change.

`values_callable` is not optional here. SQLAlchemy's default is to persist the
enum **member names** (`QUEUED`), not the values (`queued`). Without it the
database would hold `QUEUED` while the API returns `queued` and ClickHouse
stores `queued`, producing three spellings of the same state. Explicitly
selecting `.value` keeps the database, the JSON API and the column store
agreeing — and keeps the stored text readable, which is also why option 4 was
rejected.

## Consequences

### Positive

- Adding a value is a one-line constraint change; `status` has already been read
  and written under several names during development without a `CREATE TYPE`.
- The database rejects invalid values, so a bug in the worker or a manual `UPDATE`
  cannot write a state the API cannot represent.
- Values read as text in `psql`, in ClickHouse, and in the API — one vocabulary.
- No `CREATE TYPE` ordering problem in migrations, so rollbacks are ordinary
  `ALTER TABLE`s.

### Negative

- Each migration that adds a value must remember to update the `CHECK`
  constraint; forgetting it produces a runtime `IntegrityError` rather than a
  compile-time error.
- 20 bytes per value instead of a small integer, which matters only at a scale
  this system does not aim for.
- No strong typing: the database will happily compare the column to a string
  that is not in the constraint, and the plan can be worse than for a native
  enum.

### Risks

- **A constraint that drifts from the Python enum.** Mitigated by keeping both in
  the same file (`app/models.py`), so a change to one is visible next to the
  other in review, and by exercising every value in the test suite.
- **Comparing against a column with a mismatched type.** Not a correctness risk,
  but a query-performance one; the indexes on these columns are only useful if
  predicates are written against the stored values.

## Implementation Notes

- `app/models.py`: `_enum` and the `Status`, `Channel`, `Outcome` enums.
- The API rejects unknown values at the edge (`422`) via Pydantic, so the
  constraint is a backstop rather than the primary validation.
- ClickHouse uses `LowCardinality(String)` for `channel` and `event`; the
  correspondence to these values is by convention, and a new value must be
  considered in both places.

## Related Decisions

- ADR-0007: the pagination query that orders by `created_at` alongside these
  columns.

## References

- `app/models.py`, `alembic/versions/2b6d6b2559d1_initial_schema.py`
- [PostgreSQL `ALTER TYPE`](https://www.postgresql.org/docs/current/sql-altertype.html)

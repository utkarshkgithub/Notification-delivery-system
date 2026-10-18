"""ClickHouse delivery event store.

Postgres holds the current state of a notification; ClickHouse holds the
history of everything that happened to it. That split is the point - the
Postgres tables stay small and indexable no matter how many million attempts
have happened.

Analytics must never be able to fail a delivery, so writes are best effort: a
failed connection disables the sink for a cooldown window rather than retrying
(and stalling) once per event. Reads are different - a caller asking for
analytics gets a 503 instead of an empty list that looks like real data.
"""

import logging
import threading
import time
import uuid
from datetime import datetime, timezone
from urllib.parse import urlparse

import clickhouse_connect

from app.config import settings
from app.metrics import CLICKHOUSE_WRITE_ERRORS

log = logging.getLogger(__name__)

TABLE = "delivery_events"
COOLDOWN_SECONDS = 30
# One failed insert is not an outage. Without this, a single per-call error -
# which under load is routine - disabled analytics for the whole cooldown and
# silently dropped everything in between.
FAILURES_BEFORE_COOLDOWN = 5

DDL = """
CREATE TABLE IF NOT EXISTS {db}.{table} (
    event_time      DateTime64(3),
    tenant_id       UUID,
    notification_id UUID,
    channel         LowCardinality(String),
    event           LowCardinality(String),
    attempt         UInt8,
    outcome         LowCardinality(String),
    duration_ms     UInt32,
    status_code     UInt16,
    error           String
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(event_time)
ORDER BY (tenant_id, event_time, notification_id)
TTL toDateTime(event_time) + INTERVAL 90 DAY
"""

SUMMARY_SQL = """
SELECT
    event,
    count()          AS total,
    avg(duration_ms) AS avg_duration_ms,
    max(event_time)  AS last_seen
FROM {db}.{table}
WHERE tenant_id = {{tenant_id:UUID}}
  AND event_time >= now() - INTERVAL {{window:UInt32}} SECOND
GROUP BY event
ORDER BY total DESC
"""

COLUMNS = [
    "event_time",
    "tenant_id",
    "notification_id",
    "channel",
    "event",
    "attempt",
    "outcome",
    "duration_ms",
    "status_code",
    "error",
]


class ClickHouseUnavailable(RuntimeError):
    pass


# Thread-local, not a module global: a clickhouse-connect client holds a single
# session and raises if two threads use it at once. The worker runs threads, so
# a shared client meant "dropped events" as soon as deliveries overlapped.
_local = threading.local()
_disabled_until = 0.0
_schema_ready = False
_consecutive_failures = 0
_schema_lock = threading.Lock()
_failure_lock = threading.Lock()


def get_client():
    client = getattr(_local, "client", None)
    if client is None:
        parsed = urlparse(settings.clickhouse_url)
        client = clickhouse_connect.get_client(
            host=parsed.hostname or "localhost",
            port=parsed.port or (8443 if parsed.scheme == "https" else 8123),
            username=settings.clickhouse_user,
            password=settings.clickhouse_password,
            database=settings.clickhouse_database,
            secure=parsed.scheme == "https",
            connect_timeout=3,
            send_receive_timeout=5,
        )
        _local.client = client
    return client


def ensure_schema() -> None:
    global _schema_ready
    if _schema_ready:
        return
    with _schema_lock:
        if _schema_ready:
            return
        client = get_client()
        client.command(f"CREATE DATABASE IF NOT EXISTS {settings.clickhouse_database}")
        client.command(DDL.format(db=settings.clickhouse_database, table=TABLE))
        _schema_ready = True


def record(
    *,
    event: str,
    tenant_id: uuid.UUID,
    notification_id: uuid.UUID,
    channel: str,
    attempt: int = 0,
    outcome: str = "",
    duration_ms: int = 0,
    status_code: int | None = None,
    error: str | None = None,
) -> None:
    """Append one event. Never raises: the caller is the delivery path."""
    if not settings.clickhouse_enabled or time.monotonic() < _disabled_until:
        return
    try:
        ensure_schema()
        get_client().insert(
            TABLE,
            [
                [
                    datetime.now(timezone.utc),
                    tenant_id,
                    notification_id,
                    channel,
                    event,
                    attempt,
                    outcome,
                    duration_ms,
                    status_code or 0,
                    (error or "")[:2000],
                ]
            ],
            column_names=COLUMNS,
            # async_insert lets the server merge these one-row inserts into
            # shared parts instead of writing a part per row.
            #
            # wait_for_async_insert=0 is deliberate and load bearing. Waiting
            # for the flush makes every single-row insert sit on the server's
            # async buffer timeout, measured here at 85-220ms against 15ms to
            # fire and forget. This is a best effort analytics sink, so paying
            # that per event slows down the very paths it is meant to observe.
            # The cost is that only connection and acceptance errors raise;
            # a failure during the server side flush is not seen, which is the
            # right trade for a store that may never break delivery.
            settings={"async_insert": 1, "wait_for_async_insert": 0},
        )
    except Exception as exc:  # noqa: BLE001 - see module docstring
        CLICKHOUSE_WRITE_ERRORS.inc()
        _note_failure(exc)
        return
    _note_success()


def event_summary(tenant_id: uuid.UUID, window_seconds: int) -> list[dict]:
    if not settings.clickhouse_enabled or time.monotonic() < _disabled_until:
        raise ClickHouseUnavailable("event store is disabled or cooling down")
    try:
        ensure_schema()
        result = get_client().query(
            SUMMARY_SQL.format(db=settings.clickhouse_database, table=TABLE),
            parameters={"tenant_id": str(tenant_id), "window": window_seconds},
        )
    except Exception as exc:  # noqa: BLE001
        _note_failure(exc)
        raise ClickHouseUnavailable(str(exc)) from exc
    return [dict(zip(result.column_names, row)) for row in result.result_rows]


def _note_failure(exc: Exception) -> None:
    """Count the miss, and only pause once the sink looks genuinely down."""
    global _disabled_until, _schema_ready, _consecutive_failures
    with _failure_lock:
        _consecutive_failures += 1
        if _consecutive_failures < FAILURES_BEFORE_COOLDOWN:
            log.warning(
                "clickhouse write failed (%s of %s before pausing): %s",
                _consecutive_failures,
                FAILURES_BEFORE_COOLDOWN,
                exc,
            )
            return
        _consecutive_failures = 0
        _disabled_until = time.monotonic() + COOLDOWN_SECONDS
        _schema_ready = False
    log.warning("clickhouse unavailable, pausing analytics for %ss: %s", COOLDOWN_SECONDS, exc)
    _drop_client()


def _note_success() -> None:
    global _consecutive_failures
    if _consecutive_failures:
        with _failure_lock:
            _consecutive_failures = 0


def _drop_client() -> None:
    """Drop this thread's connection so the next attempt starts clean."""
    client = getattr(_local, "client", None)
    if client is not None:
        try:
            client.close()
        except Exception:  # noqa: BLE001
            pass
        _local.client = None

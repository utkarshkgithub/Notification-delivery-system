"""Analytics sink tests.

The contract being tested: analytics may never break delivery, and a read must
never pretend that "the store is down" is "there is no data".
"""

import uuid

import pytest

from app import events
from app.config import settings
from app.events import ClickHouseUnavailable


def test_disabled_sink_is_a_no_op(monkeypatch):
    monkeypatch.setattr(settings, "clickhouse_enabled", False)

    def explode():
        raise AssertionError("a disabled sink must not connect")

    monkeypatch.setattr(events, "get_client", explode)

    events.record(
        event="delivered",
        tenant_id=uuid.uuid4(),
        notification_id=uuid.uuid4(),
        channel="in_app",
    )


def test_disabled_sink_refuses_to_answer_reads(monkeypatch):
    monkeypatch.setattr(settings, "clickhouse_enabled", False)

    with pytest.raises(ClickHouseUnavailable):
        events.event_summary(uuid.uuid4(), 3600)


def test_write_failure_is_swallowed_and_counted(monkeypatch):
    monkeypatch.setattr(settings, "clickhouse_enabled", True)
    calls = {"n": 0}

    def broken():
        calls["n"] += 1
        raise ClickHouseUnavailable("connection refused")

    monkeypatch.setattr(events, "get_client", broken)

    events.record(
        event="delivered",
        tenant_id=uuid.uuid4(),
        notification_id=uuid.uuid4(),
        channel="in_app",
    )
    events.record(
        event="delivered",
        tenant_id=uuid.uuid4(),
        notification_id=uuid.uuid4(),
        channel="in_app",
    )

    assert calls["n"] == 1, "the cooldown must stop a retry storm, not retry per event"


def test_cooldown_expires_and_the_sink_recovers(monkeypatch):
    monkeypatch.setattr(settings, "clickhouse_enabled", True)
    monkeypatch.setattr(events, "_disabled_until", 0.0)
    attempts = {"n": 0}

    def broken():
        attempts["n"] += 1
        raise ClickHouseUnavailable("connection refused")

    monkeypatch.setattr(events, "get_client", broken)
    events.record(event="dead", tenant_id=uuid.uuid4(), notification_id=uuid.uuid4(), channel="in_app")
    assert attempts["n"] == 1

    # Time passes: the sink is tried again rather than staying off forever.
    monkeypatch.setattr(events, "_disabled_until", 0.0)
    events.record(event="dead", tenant_id=uuid.uuid4(), notification_id=uuid.uuid4(), channel="in_app")
    assert attempts["n"] == 2


def test_event_stats_returns_503_when_the_store_is_down(client, auth, monkeypatch):
    def unavailable(tenant_id, window_seconds):
        raise ClickHouseUnavailable("event store is disabled or cooling down")

    monkeypatch.setattr("app.routes.events.event_summary", unavailable)

    response = client.get("/v1/stats/events", headers=auth)

    assert response.status_code == 503
    assert "unavailable" in response.json()["detail"]


def test_event_stats_shape(client, auth, monkeypatch):
    from datetime import datetime, timezone

    monkeypatch.setattr(
        "app.routes.events.event_summary",
        lambda tenant_id, window_seconds: [
            {
                "event": "delivered",
                "total": 12,
                "avg_duration_ms": 3.5,
                "last_seen": datetime(2026, 1, 1, tzinfo=timezone.utc),
            }
        ],
    )

    response = client.get("/v1/stats/events", params={"window_seconds": 600}, headers=auth)

    assert response.status_code == 200
    body = response.json()
    assert body["window_seconds"] == 600
    assert body["events"] == [
        {
            "event": "delivered",
            "total": 12,
            "avg_duration_ms": 3.5,
            "last_seen": "2026-01-01T00:00:00Z",
        }
    ]

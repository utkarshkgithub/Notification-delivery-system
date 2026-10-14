"""Pipeline tests: real Postgres, real cascade of state transitions, stubbed
transport. Every assertion here is about ordering, idempotency or durability."""

from datetime import datetime, timedelta, timezone

import pytest
from sqlalchemy import select

from app.cache import cache_get, cache_set, notification_key
from app.channels import DeliveryResult, PermanentFailure
from app.db import session_scope
from app.models import Channel, DeliveryAttempt, Notification, Outcome, Status
from app.tasks import (
    attempt_delivery,
    backoff_seconds,
    reclaim_stuck_deliveries,
    sweep_queued_notifications,
)


@pytest.fixture
def sent(monkeypatch):
    """Stubs only the transport. Everything else - transactions, retries,
    dead-lettering, cache invalidation - is the real code."""

    class Transport:
        def __init__(self) -> None:
            self.calls: list[str] = []
            self.result: DeliveryResult = DeliveryResult(ok=True, status_code=200, duration_ms=7)

        def __call__(self, notification, tenant):
            self.calls.append(str(notification.id))
            if isinstance(self.result, Exception):
                raise self.result
            return self.result

    transport = Transport()
    monkeypatch.setattr("app.tasks.deliver", transport)
    return transport


def _attempts(notification_id) -> list[DeliveryAttempt]:
    with session_scope() as session:
        return list(
            session.scalars(
                select(DeliveryAttempt)
                .where(DeliveryAttempt.notification_id == notification_id)
                .order_by(DeliveryAttempt.attempt)
            )
        )


def test_successful_delivery_records_attempt(sent, notification_factory):
    notification = notification_factory()
    outcome = attempt_delivery(notification.id)

    assert outcome.delivered is True
    assert outcome.retry_in is None
    assert sent.calls == [str(notification.id)]

    with session_scope() as session:
        row = session.get(Notification, notification.id)
        assert row.status is Status.DELIVERED
        assert row.attempts == 1
        assert row.delivered_at is not None
        assert row.last_error is None

    attempts = _attempts(notification.id)
    assert len(attempts) == 1
    assert attempts[0].outcome is Outcome.DELIVERED
    assert attempts[0].status_code == 200
    assert attempts[0].duration_ms == 7


def test_retryable_failure_schedules_retry(sent, notification_factory):
    sent.result = DeliveryResult(ok=False, status_code=503, error="receiver returned 503")
    notification = notification_factory()

    before = datetime.now(timezone.utc)
    outcome = attempt_delivery(notification.id)

    assert outcome.delivered is False
    assert outcome.retry_in is not None
    # Equal jitter: between 50% and 100% of the 2s first-attempt delay.
    assert 1.0 <= outcome.retry_in <= 2.0

    with session_scope() as session:
        row = session.get(Notification, notification.id)
        assert row.status is Status.QUEUED
        assert row.attempts == 1
        assert row.last_error == "receiver returned 503"
        assert row.next_attempt_at > before

    assert _attempts(notification.id)[0].outcome is Outcome.FAILED


def test_permanent_failure_dead_letters_without_retrying(sent, notification_factory):
    sent.result = PermanentFailure("receiver rejected the delivery with 400", status_code=400)
    notification = notification_factory()

    outcome = attempt_delivery(notification.id)

    assert outcome.delivered is False
    assert outcome.retry_in is None, "a 4xx must not be retried"
    assert len(sent.calls) == 1

    with session_scope() as session:
        row = session.get(Notification, notification.id)
        assert row.status is Status.DEAD
        assert row.last_status_code == 400
        assert "rejected permanently" in row.last_error
        assert "400" in row.last_error, "the receiver's answer must survive"

    assert _attempts(notification.id)[0].outcome is Outcome.PERMANENT


def test_exhausted_attempts_go_to_dead_letter(sent, notification_factory):
    sent.result = DeliveryResult(ok=False, status_code=500, error="server error")
    notification = notification_factory(attempts=4, max_attempts=5)

    outcome = attempt_delivery(notification.id)

    assert outcome.retry_in is None
    with session_scope() as session:
        row = session.get(Notification, notification.id)
        assert row.status is Status.DEAD
        assert row.attempts == 5
        # dead after five tries must be distinguishable from dead after one
        assert "exhausted after 5" in row.last_error
        assert "server error" in row.last_error


def test_delivered_notification_is_not_sent_again(sent, notification_factory):
    notification = notification_factory(status=Status.DELIVERED)

    outcome = attempt_delivery(notification.id)

    assert outcome.skipped is True
    assert sent.calls == [], "a duplicate task must not re-send"


def test_in_flight_delivery_is_not_sent_again(sent, notification_factory):
    """The double-send guard: a redelivered message while an attempt is still
    running must return, not send twice."""
    notification = notification_factory(status=Status.DELIVERING)

    outcome = attempt_delivery(notification.id)

    assert outcome.skipped is True
    assert sent.calls == []


def test_stale_in_flight_delivery_is_retried(sent, notification_factory):
    """Once the attempt is older than the stuck timeout, it is fair game again."""
    notification = notification_factory(status=Status.DELIVERING)
    with session_scope() as session:
        session.get(Notification, notification.id).updated_at = datetime.now(
            timezone.utc
        ) - timedelta(hours=1)

    outcome = attempt_delivery(notification.id)

    assert outcome.delivered is True
    assert sent.calls == [str(notification.id)]


def test_missing_notification_is_ignored(sent):
    import uuid

    outcome = attempt_delivery(uuid.uuid4())
    assert outcome.skipped is True
    assert sent.calls == []


def test_state_change_invalidates_the_cache(sent, notification_factory):
    notification = notification_factory()
    key = notification_key(notification.id)
    cache_set(key, {"status": "queued"}, 60)
    assert cache_get(key) is not None

    attempt_delivery(notification.id)

    assert cache_get(key) is None, "a delivered notification must not stay cached as queued"


def test_crashed_delivery_is_reclaimed(sent, notification_factory, enqueued):
    notification = notification_factory(status=Status.DELIVERING)
    with session_scope() as session:
        session.get(Notification, notification.id).updated_at = datetime.now(
            timezone.utc
        ) - timedelta(minutes=10)

    count = reclaim_stuck_deliveries(timeout_seconds=300)

    assert count == 1
    assert enqueued == [str(notification.id)]
    with session_scope() as session:
        row = session.get(Notification, notification.id)
        assert row.status is Status.QUEUED
        assert row.last_error == "reclaimed after worker timeout"


def test_reclaimer_leaves_healthy_in_flight_deliveries_alone(sent, notification_factory, enqueued):
    notification = notification_factory(status=Status.DELIVERING)

    assert reclaim_stuck_deliveries(timeout_seconds=300) == 0
    assert enqueued == []
    with session_scope() as session:
        assert session.get(Notification, notification.id).status is Status.DELIVERING


def test_sweeper_republishes_due_but_untouched(sent, notification_factory, enqueued):
    notification = notification_factory()
    with session_scope() as session:
        session.get(Notification, notification.id).next_attempt_at = datetime.now(
            timezone.utc
        ) - timedelta(minutes=5)

    count = sweep_queued_notifications(grace_seconds=120)

    assert count == 1
    assert enqueued == [str(notification.id)]


def test_sweeper_skips_recent_and_scheduled_notifications(sent, notification_factory, enqueued):
    fresh = notification_factory()
    scheduled = notification_factory()
    with session_scope() as session:
        # Inside the grace window: still on its way through the queue.
        session.get(Notification, fresh.id).next_attempt_at = datetime.now(timezone.utc)
        # Future retry: the broker already holds a message with a countdown.
        session.get(Notification, scheduled.id).next_attempt_at = datetime.now(
            timezone.utc
        ) + timedelta(minutes=5)

    assert sweep_queued_notifications(grace_seconds=120) == 0
    assert enqueued == []


def test_backoff_grows_and_is_capped():
    delays = [backoff_seconds(attempt, base=2, cap=300) for attempt in range(1, 9)]

    for attempt, delay in enumerate(delays, start=1):
        ideal = min(300, 2 * 2 ** (attempt - 1))
        assert ideal * 0.5 <= delay <= ideal, f"attempt {attempt} out of jitter range"
    assert delays[-1] <= 300


def test_backoff_jitter_spreads_a_batch():
    samples = {round(backoff_seconds(3, base=2, cap=300), 3) for _ in range(20)}
    assert len(samples) > 1, "a fixed delay would stampede a recovering receiver"

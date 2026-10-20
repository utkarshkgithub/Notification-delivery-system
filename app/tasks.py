"""Celery worker: the delivery pipeline.

Delivery is at-least-once. Two guards keep that from turning into duplicate
sends: a task re-enqueued while another attempt is still in flight sees status
`delivering` and returns without sending, and every retry is a fresh message
rather than a broker redelivery. A worker killed mid-send leaves the row in
`delivering`; the reclaimer puts it back in the queue after
STUCK_DELIVERY_TIMEOUT_SECONDS.
"""

import logging
import random
import time
import uuid
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone

from celery import Celery
from celery.signals import worker_ready
from sqlalchemy import func, select, update

from app import events
from app.cache import cache_delete, notification_key
from app.channels import PermanentFailure, deliver
from app.config import settings
from app.db import session_scope
from app.metrics import (
    DEAD_LETTERED,
    DELIVERY_ATTEMPTS,
    DELIVERY_DURATION,
    RETRIES_SCHEDULED,
    STUCK_RECLAIMED,
    SWEEP_REQUEUED,
)
from app.models import DeliveryAttempt, Notification, Outcome, Status

log = logging.getLogger(__name__)

celery_app = Celery("notification_infra", broker=settings.redis_url, backend=settings.redis_url)
celery_app.conf.update(
    task_serializer="json",
    result_serializer="json",
    accept_content=["json"],
    timezone="UTC",
    enable_utc=True,
    # acks_late + reject_on_worker_lost: a hard kill returns the message to the
    # queue instead of losing the delivery.
    task_acks_late=True,
    task_reject_on_worker_lost=True,
    # 1 is the safe-looking default and it throttles this stack hard: with
    # acks_late on the Redis broker, a multiplier of 1 keeps roughly one
    # message in flight, so the pool sits idle while a task takes milliseconds.
    # Measured on a drained queue, 1000 in_app notifications at concurrency 16:
    # 191.0s to drain at 1 versus 3.7s at 8 - 51x - with ingest identical at
    # ~111 req/s in both arms and the worker under one core. See the README.
    # Prefetching more only widens the window in which
    # a crashed worker has unacked messages, and a redelivery of those is
    # harmless here - attempt_delivery skips anything already in flight or
    # already finished.
    worker_prefetch_multiplier=settings.worker_prefetch_multiplier,
    # Must stay above the largest retry delay, or Redis redelivers a task whose
    # countdown has not elapsed yet.
    broker_transport_options={"visibility_timeout": 1800},
    result_expires=3600,
    beat_schedule={
        "reclaim-stuck-deliveries": {
            "task": "app.tasks.reclaim_stuck_deliveries",
            "schedule": settings.reclaimer_interval_seconds,
        },
        "sweep-queued-notifications": {
            "task": "app.tasks.sweep_queued_notifications",
            "schedule": settings.sweeper_interval_seconds,
        },
    },
)


def backoff_seconds(attempt: int, base: float | None = None, cap: float | None = None) -> float:
    """Exponential backoff with equal jitter, attempt is 1-based.

    Jitter is between 50% and 100% of the ideal delay: wide enough to stop a
    batch of simultaneous failures from stampeding the receiver, narrow enough
    that a later attempt is still scheduled later than an earlier one.
    """
    base = settings.retry_base_seconds if base is None else base
    cap = settings.retry_cap_seconds if cap is None else cap
    ideal = min(cap, base * (2 ** (attempt - 1)))
    return ideal * (0.5 + random.random() / 2)


@dataclass
class AttemptOutcome:
    delivered: bool
    retry_in: float | None = None
    skipped: bool = False


def _now() -> datetime:
    return datetime.now(timezone.utc)


@celery_app.task(bind=True, name="app.tasks.deliver_notification", acks_late=True, max_retries=None)
def deliver_notification(self, notification_id: str) -> bool:
    outcome = attempt_delivery(notification_id)
    if outcome.retry_in is not None:
        raise self.retry(countdown=outcome.retry_in)
    return outcome.delivered


def attempt_delivery(notification_id: str | uuid.UUID) -> AttemptOutcome:
    """One delivery attempt, start to finish.

    A plain function so tests can drive the real pipeline without a broker.
    """
    notification_id = uuid.UUID(str(notification_id))

    with session_scope() as session:
        notification = session.get(Notification, notification_id)
        if notification is None:
            log.warning("notification %s no longer exists, dropping task", notification_id)
            return AttemptOutcome(delivered=False, skipped=True)
        if notification.status in (Status.DELIVERED, Status.DEAD):
            # Duplicate task, or a broker redelivery after we already finished.
            return AttemptOutcome(delivered=notification.status is Status.DELIVERED, skipped=True)
        if notification.status is Status.DELIVERING and notification.updated_at > _now() - timedelta(
            seconds=settings.stuck_delivery_timeout_seconds
        ):
            # Another attempt is in flight. Sending now would double-deliver.
            return AttemptOutcome(delivered=False, skipped=True)
        if notification.attempts >= notification.max_attempts:
            return _dead_letter(
                session, notification, "attempts already exhausted when picked up"
            )

        channel = notification.channel.value
        tenant = notification.tenant
        notification.status = Status.DELIVERING
        notification.attempts += 1
        attempt_no = notification.attempts
        # Commit before the network call: holding an open transaction across a
        # webhook timeout means holding a row lock for the whole timeout.
        session.commit()

        started = time.perf_counter()
        try:
            result = deliver(notification, tenant)
        except PermanentFailure as exc:
            outcome: Outcome | None = Outcome.PERMANENT
            status_code, error, duration_ms = exc.status_code, str(exc), exc.duration_ms
        else:
            outcome = Outcome.DELIVERED if result.ok else Outcome.FAILED
            status_code, error, duration_ms = result.status_code, result.error, result.duration_ms
        measured_ms = int((time.perf_counter() - started) * 1000)
        duration_ms = duration_ms or measured_ms

        DELIVERY_DURATION.labels(channel=channel).observe(measured_ms / 1000)
        DELIVERY_ATTEMPTS.labels(channel=channel, outcome=outcome.value).inc()
        session.add(
            DeliveryAttempt(
                notification_id=notification.id,
                attempt=attempt_no,
                outcome=outcome,
                status_code=status_code,
                error=error,
                duration_ms=duration_ms,
            )
        )
        notification.last_status_code = status_code
        notification.last_error = error

        if outcome is Outcome.DELIVERED:
            notification.status = Status.DELIVERED
            notification.delivered_at = _now()
            notification.last_error = None
            session.commit()
            _after_commit(notification, "delivered", attempt_no, outcome, duration_ms, status_code, None)
            return AttemptOutcome(delivered=True)

        if outcome is Outcome.PERMANENT or attempt_no >= notification.max_attempts:
            # last_error records why it stopped being retried. The transport's
            # own message is already in delivery_attempts, so repeating it here
            # would lose the distinction an operator actually needs: dead after
            # one rejection, or dead after five tries.
            reason = (
                f"rejected permanently: {error}"
                if outcome is Outcome.PERMANENT
                else f"attempts exhausted after {attempt_no}: {error}"
            )
            session.commit()  # persist the attempt row before flipping to dead
            return _dead_letter(session, notification, reason, attempt_no, outcome, duration_ms, status_code)

        delay = backoff_seconds(attempt_no)
        notification.status = Status.QUEUED
        notification.next_attempt_at = _now() + timedelta(seconds=delay)
        session.commit()
        RETRIES_SCHEDULED.labels(channel=channel).inc()
        _after_commit(notification, "retry_scheduled", attempt_no, outcome, duration_ms, status_code, error)
        log.info(
            "notification %s attempt %s/%s failed (%s), retrying in %.1fs",
            notification.id,
            attempt_no,
            notification.max_attempts,
            error,
            delay,
        )
        return AttemptOutcome(delivered=False, retry_in=delay)


def _dead_letter(
    session,
    notification: Notification,
    reason: str,
    attempt_no: int = 0,
    outcome: Outcome | None = None,
    duration_ms: int = 0,
    status_code: int | None = None,
) -> AttemptOutcome:
    notification.status = Status.DEAD
    notification.last_error = reason
    session.commit()
    DEAD_LETTERED.labels(channel=notification.channel.value).inc()
    _after_commit(notification, "dead", attempt_no, outcome, duration_ms, status_code, reason)
    log.warning("notification %s moved to dead letter: %s", notification.id, reason)
    return AttemptOutcome(delivered=False)


def _after_commit(
    notification: Notification,
    event: str,
    attempt_no: int,
    outcome: Outcome | None,
    duration_ms: int,
    status_code: int | None,
    error: str | None,
) -> None:
    """Side effects that must not run inside the write transaction."""
    cache_delete(notification_key(notification.id))
    events.record(
        event=event,
        tenant_id=notification.tenant_id,
        notification_id=notification.id,
        channel=notification.channel.value,
        attempt=attempt_no,
        outcome=outcome.value if outcome else "",
        duration_ms=duration_ms,
        status_code=status_code,
        error=error,
    )


@celery_app.task(name="app.tasks.reclaim_stuck_deliveries")
def reclaim_stuck_deliveries(timeout_seconds: int | None = None) -> int:
    """Re-queue deliveries whose worker died mid-flight."""
    timeout_seconds = timeout_seconds or settings.stuck_delivery_timeout_seconds
    cutoff = _now() - timedelta(seconds=timeout_seconds)
    with session_scope() as session:
        reclaimed = (
            session.execute(
                update(Notification)
                .where(Notification.status == Status.DELIVERING, Notification.updated_at < cutoff)
                .values(
                    status=Status.QUEUED,
                    next_attempt_at=func.now(),
                    last_error="reclaimed after worker timeout",
                )
                .returning(Notification.id)
            )
            .scalars()
            .all()
        )
    # Enqueue only after the reset is committed, or a worker could load the row
    # and still see the old status.
    for notification_id in reclaimed:
        STUCK_RECLAIMED.inc()
        deliver_notification.apply_async(args=[str(notification_id)])
    if reclaimed:
        log.warning("reclaimed %s stuck deliveries", len(reclaimed))
    return len(reclaimed)


@celery_app.task(name="app.tasks.sweep_queued_notifications")
def sweep_queued_notifications(grace_seconds: int | None = None, batch: int = 500) -> int:
    """Re-publish notifications that are due but have not been picked up.

    The broker is Redis, so a flush or a dropped message would otherwise lose an
    accepted notification with no trace. The reclaimer covers a worker dying
    mid-flight; this covers the message never arriving at all. Both are safe to
    re-run because attempt_delivery ignores anything already terminal or in
    flight.

    next_attempt_at is pushed forward so the same batch is not re-published
    every interval while it is on its way through the queue.
    """
    grace = settings.queued_sweep_grace_seconds if grace_seconds is None else grace_seconds
    cutoff = _now() - timedelta(seconds=grace)
    with session_scope() as session:
        due = (
            session.scalars(
                select(Notification.id)
                .where(Notification.status == Status.QUEUED, Notification.next_attempt_at < cutoff)
                .order_by(Notification.next_attempt_at)
                .limit(batch)
            ).all()
        )
        if due:
            session.execute(
                update(Notification).where(Notification.id.in_(due)).values(next_attempt_at=func.now())
            )
    for notification_id in due:
        SWEEP_REQUEUED.inc()
        deliver_notification.apply_async(args=[str(notification_id)])
    if due:
        log.warning("sweeper re-published %s due-but-untouched notifications", len(due))
    return len(due)


@worker_ready.connect
def _start_metrics_server(**_kwargs) -> None:
    """Scraped separately from the API, whose /metrics only covers its own
    process."""
    from prometheus_client import start_http_server

    start_http_server(settings.metrics_port)
    log.info("worker metrics on :%s", settings.metrics_port)

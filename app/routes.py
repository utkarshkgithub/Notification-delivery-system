"""The REST API.

Conventions: /v1 prefix, UUID path params, cursor pagination, 202 for work that
continues in the background, 409 for state conflicts, 429 with Retry-After when
a tenant is over its limit.
"""

import base64
import binascii
import json
import logging
import uuid
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone

from fastapi import APIRouter, Depends, Header, HTTPException, Query, Response
from sqlalchemy import func, select, tuple_
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from app import events
from app.cache import cache_delete, cache_get, cache_set, notification_key
from app.config import settings
from app.db import get_session
from app.metrics import NOTIFICATIONS_CREATED
from app.models import Channel, Notification, Status, Tenant
from app.ratelimit import check_rate_limit
from app.schemas import (
    EventCount,
    EventStatsOut,
    NotificationCreate,
    NotificationDetail,
    NotificationOut,
    Page,
    StatsOut,
    StatusCounts,
)
from app.security import hash_api_key
from app.tasks import deliver_notification

log = logging.getLogger(__name__)

router = APIRouter(prefix="/v1", tags=["notifications"])


@dataclass(frozen=True)
class TenantAuth:
    """What the request path needs from a tenant, and nothing else. Caching
    this instead of the ORM row keeps authentication off Postgres entirely on a
    cache hit - the webhook secret is only ever read by the worker."""

    id: uuid.UUID
    rate_limit_per_minute: int | None


def current_tenant(
    response: Response,
    authorization: str | None = Header(default=None),
    session: Session = Depends(get_session),
) -> TenantAuth:
    """Authenticates, then rate limits. Every endpoint that takes a tenant is
    limited by construction - there is no way to add a route and forget the
    limiter."""
    scheme, _, token = (authorization or "").partition(" ")
    if scheme.lower() != "bearer" or not token.strip():
        raise HTTPException(401, "expected an Authorization: Bearer <api key> header")
    key_hash = hash_api_key(token.strip())

    cache_key = f"tenant:v1:{key_hash}"
    cached = cache_get(cache_key)
    if cached is not None:
        tenant = TenantAuth(uuid.UUID(cached["id"]), cached["rate_limit_per_minute"])
    else:
        row = session.scalar(select(Tenant).where(Tenant.api_key_hash == key_hash))
        if row is None:
            raise HTTPException(401, "invalid API key")
        tenant = TenantAuth(row.id, row.rate_limit_per_minute)
        cache_set(
            cache_key,
            {"id": str(row.id), "rate_limit_per_minute": row.rate_limit_per_minute},
            settings.tenant_cache_ttl_seconds,
        )

    limit = tenant.rate_limit_per_minute or settings.rate_limit_per_minute
    allowed, remaining, retry_after = check_rate_limit(
        tenant.id, limit, settings.rate_limit_burst
    )
    response.headers["X-RateLimit-Limit"] = str(limit)
    response.headers["X-RateLimit-Remaining"] = str(remaining)
    if not allowed:
        raise HTTPException(
            429,
            "rate limit exceeded",
            headers={"Retry-After": str(max(retry_after, 1))},
        )
    return tenant


@router.post("/notifications", response_model=NotificationOut, status_code=202)
def create_notification(
    payload: NotificationCreate,
    response: Response,
    idempotency_key: str | None = Header(default=None, alias="Idempotency-Key", max_length=255),
    tenant: TenantAuth = Depends(current_tenant),
    session: Session = Depends(get_session),
) -> Notification:
    """Accepts a notification and hands it to the queue. The response is 202
    because nothing has been delivered yet; poll GET /v1/notifications/{id}."""
    if idempotency_key:
        existing = _by_idempotency_key(session, tenant.id, idempotency_key)
        if existing is not None:
            response.status_code = 200
            return existing

    notification = Notification(
        tenant_id=tenant.id,
        channel=payload.channel,
        recipient=payload.recipient,
        subject=payload.subject,
        body=payload.body,
        payload=payload.payload,
        idempotency_key=idempotency_key,
        max_attempts=payload.max_attempts or settings.max_attempts_default,
    )
    session.add(notification)
    try:
        session.commit()
    except IntegrityError:
        # Two requests with the same key raced; the unique index picked a
        # winner, so return the winner rather than a 500.
        session.rollback()
        existing = _by_idempotency_key(session, tenant.id, idempotency_key)
        if existing is None:
            raise
        response.status_code = 200
        return existing

    NOTIFICATIONS_CREATED.labels(channel=notification.channel.value).inc()
    events.record(
        event="queued",
        tenant_id=notification.tenant_id,
        notification_id=notification.id,
        channel=notification.channel.value,
    )
    # Enqueue after the commit, never before: a worker that cannot see the row
    # would drop the delivery.
    deliver_notification.apply_async(args=[str(notification.id)])
    return notification


@router.get("/notifications/{notification_id}", response_model=NotificationDetail)
def get_notification(
    notification_id: uuid.UUID,
    tenant: TenantAuth = Depends(current_tenant),
    session: Session = Depends(get_session),
) -> NotificationDetail | dict:
    """Cache-aside read. The entry is deleted by the worker on every state
    change, so a stale value can only survive for one TTL."""
    cache_key = notification_key(notification_id)
    cached = cache_get(cache_key)
    if cached is not None:
        return cached

    notification = session.scalar(
        select(Notification).where(
            Notification.id == notification_id, Notification.tenant_id == tenant.id
        )
    )
    if notification is None:
        raise HTTPException(404, "notification not found")
    detail = NotificationDetail.model_validate(notification)
    cache_set(cache_key, detail.model_dump(mode="json"), settings.notification_cache_ttl_seconds)
    return detail


@router.get("/notifications", response_model=Page)
def list_notifications(
    status_filter: Status | None = Query(default=None, alias="status"),
    channel: Channel | None = None,
    limit: int = Query(default=50, ge=1, le=200),
    cursor: str | None = None,
    tenant: TenantAuth = Depends(current_tenant),
    session: Session = Depends(get_session),
) -> Page:
    """Keyset pagination, not OFFSET: deep pages stay a single index seek on
    (tenant_id, created_at DESC, id DESC) instead of scanning and discarding
    every earlier row."""
    stmt = select(Notification).where(Notification.tenant_id == tenant.id)
    if status_filter is not None:
        stmt = stmt.where(Notification.status == status_filter)
    if channel is not None:
        stmt = stmt.where(Notification.channel == channel)
    if cursor:
        created_at, last_id = _decode_cursor(cursor)
        stmt = stmt.where(
            tuple_(Notification.created_at, Notification.id) < tuple_(created_at, last_id)
        )
    # One extra row tells us whether another page exists.
    rows = list(
        session.scalars(
            stmt.order_by(Notification.created_at.desc(), Notification.id.desc()).limit(limit + 1)
        )
    )
    next_cursor = None
    if len(rows) > limit:
        rows = rows[:limit]
        next_cursor = _encode_cursor(rows[-1].created_at, rows[-1].id)
    return Page(items=rows, next_cursor=next_cursor)


@router.post("/notifications/{notification_id}/retry", response_model=NotificationOut, status_code=202)
def retry_notification(
    notification_id: uuid.UUID,
    tenant: TenantAuth = Depends(current_tenant),
    session: Session = Depends(get_session),
) -> Notification:
    """Manual retry of a dead-lettered notification. A human has decided the
    condition that killed it is gone, so the retry budget is topped up rather
    than reset (attempt numbers stay monotonic in the log)."""
    notification = session.scalar(
        select(Notification).where(
            Notification.id == notification_id, Notification.tenant_id == tenant.id
        )
    )
    if notification is None:
        raise HTTPException(404, "notification not found")
    if notification.status is not Status.DEAD:
        raise HTTPException(409, f"only dead notifications can be retried, this one is {notification.status.value}")

    notification.status = Status.QUEUED
    notification.max_attempts = min(notification.attempts + settings.max_attempts_default, 100)
    notification.next_attempt_at = datetime.now(timezone.utc)
    session.commit()
    cache_delete(notification_key(notification.id))
    deliver_notification.apply_async(args=[str(notification.id)])
    return notification


@router.get("/stats", response_model=StatsOut)
def stats(
    window_seconds: int = Query(default=3600, ge=60, le=604800),
    tenant: TenantAuth = Depends(current_tenant),
    session: Session = Depends(get_session),
) -> StatsOut:
    """Current-state counts from Postgres, served by
    (tenant_id, status) / (tenant_id, created_at)."""
    since = datetime.now(timezone.utc) - timedelta(seconds=window_seconds)
    rows = session.execute(
        select(Notification.status, func.count())
        .where(Notification.tenant_id == tenant.id, Notification.created_at >= since)
        .group_by(Notification.status)
    ).all()
    by_status = {member.value: 0 for member in Status}
    for status_value, count in rows:
        by_status[status_value.value] = count
    return StatsOut(
        window_seconds=window_seconds,
        total=sum(by_status.values()),
        by_status=StatusCounts(**by_status),
    )


@router.get("/stats/events", response_model=EventStatsOut)
def event_stats(
    window_seconds: int = Query(default=3600, ge=60, le=604800),
    tenant: TenantAuth = Depends(current_tenant),
) -> EventStatsOut:
    """Per-event history from ClickHouse. Answers questions Postgres is a bad
    fit for, like "what is the p95 webhook latency this hour"."""
    try:
        rows = events.event_summary(tenant.id, window_seconds)
    except events.ClickHouseUnavailable as exc:
        # 503, not an empty list: an analytics outage must not look like "no
        # events happened".
        raise HTTPException(503, f"event store unavailable: {exc}") from exc
    return EventStatsOut(
        window_seconds=window_seconds,
        events=[
            EventCount(
                event=row["event"],
                total=row["total"],
                avg_duration_ms=round(float(row["avg_duration_ms"] or 0), 2),
                last_seen=row["last_seen"],
            )
            for row in rows
        ],
    )


def _by_idempotency_key(session: Session, tenant_id: uuid.UUID, key: str) -> Notification | None:
    return session.scalar(
        select(Notification).where(
            Notification.tenant_id == tenant_id, Notification.idempotency_key == key
        )
    )


def _encode_cursor(created_at: datetime, notification_id: uuid.UUID) -> str:
    raw = json.dumps({"ts": created_at.isoformat(), "id": str(notification_id)})
    return base64.urlsafe_b64encode(raw.encode()).decode()


def _decode_cursor(cursor: str) -> tuple[datetime, uuid.UUID]:
    try:
        payload = json.loads(base64.urlsafe_b64decode(cursor.encode()))
        return datetime.fromisoformat(payload["ts"]), uuid.UUID(payload["id"])
    except (ValueError, KeyError, TypeError, binascii.Error) as exc:
        raise HTTPException(400, "invalid cursor") from exc

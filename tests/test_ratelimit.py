"""Token bucket tests. The Lua script is the interesting part, so these drive
the real thing against real Redis."""

import time

import redis

from app.config import settings
from app.ratelimit import check_rate_limit

CREATED = {"channel": "in_app", "recipient": "user-1", "body": "hello"}


def test_bucket_allows_up_to_capacity_then_rejects(client, auth, monkeypatch, enqueued):
    monkeypatch.setattr(settings, "rate_limit_burst", 2)

    first = client.post("/v1/notifications", headers=auth, json=CREATED)
    second = client.post("/v1/notifications", headers=auth, json=CREATED)
    third = client.post("/v1/notifications", headers=auth, json=CREATED)

    assert [first.status_code, second.status_code, third.status_code] == [202, 202, 429]


def test_rejection_carries_the_headers_a_client_needs(client, auth, monkeypatch, enqueued):
    monkeypatch.setattr(settings, "rate_limit_burst", 1)
    client.post("/v1/notifications", headers=auth, json=CREATED)

    response = client.post("/v1/notifications", headers=auth, json=CREATED)

    assert response.status_code == 429
    assert int(response.headers["retry-after"]) >= 1
    # Regression: these used to be dropped, because HTTPException builds its
    # own response.
    assert response.headers["x-ratelimit-limit"] == str(settings.rate_limit_per_minute)
    assert response.headers["x-ratelimit-remaining"] == "0"


def test_rejected_requests_are_not_stored(client, auth, monkeypatch, enqueued):
    from sqlalchemy import text

    from app.db import session_scope

    monkeypatch.setattr(settings, "rate_limit_burst", 1)
    client.post("/v1/notifications", headers=auth, json=CREATED)
    client.post("/v1/notifications", headers=auth, json=CREATED)

    with session_scope() as session:
        count = session.execute(text("SELECT count(*) FROM notifications")).scalar()
    assert count == 1, "a 429 must not create a notification"


def test_tenant_limit_overrides_the_default(client, auth, tenant, monkeypatch, enqueued):
    from app.db import session_scope
    from app.models import Tenant

    monkeypatch.setattr(settings, "rate_limit_per_minute", 6000)
    with session_scope() as session:
        session.get(Tenant, tenant.id).rate_limit_per_minute = 120

    response = client.post("/v1/notifications", headers=auth, json=CREATED)

    assert response.headers["x-ratelimit-limit"] == "120"


def test_fails_open_when_redis_is_unavailable(client, auth, monkeypatch, enqueued):
    """A limiter outage must not reject traffic."""

    def broken():
        raise redis.ConnectionError("redis is gone")

    monkeypatch.setattr("app.ratelimit.get_redis", broken)

    response = client.post("/v1/notifications", headers=auth, json=CREATED)

    assert response.status_code == 202


def test_bucket_refills_over_time(tenant):
    allowed, remaining, retry_after = check_rate_limit(tenant.id, limit_per_minute=6000, burst=1)
    assert (allowed, remaining, retry_after) == (True, 0, 0)

    assert check_rate_limit(tenant.id, limit_per_minute=6000, burst=1)[0] is False

    # 6000/min refills a token in 10 ms.
    time.sleep(0.2)
    assert check_rate_limit(tenant.id, limit_per_minute=6000, burst=1)[0] is True


def test_retry_after_is_positive_and_bounded(tenant):
    check_rate_limit(tenant.id, limit_per_minute=60, burst=1)

    allowed, _, retry_after = check_rate_limit(tenant.id, limit_per_minute=60, burst=1)

    assert allowed is False
    assert 1 <= retry_after <= 2, "one token at 1/s is about a second away"


def test_buckets_are_isolated_per_tenant(tenant):
    import uuid

    check_rate_limit(tenant.id, limit_per_minute=60, burst=1)

    allowed, _, _ = check_rate_limit(uuid.uuid4(), limit_per_minute=60, burst=1)
    assert allowed is True, "one tenant must not exhaust another's bucket"

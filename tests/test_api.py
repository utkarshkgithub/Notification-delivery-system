"""API tests. Uses the real app, real sessions and real Redis."""

from app.db import session_scope
from app.models import Channel, Notification, Status

CREATED = {"channel": "in_app", "recipient": "user-1", "body": "hello"}


def _count(filters: str = "") -> int:
    from sqlalchemy import text

    with session_scope() as session:
        return session.execute(text(f"SELECT count(*) FROM notifications {filters}")).scalar()


def test_healthz_reports_dependencies(client):
    response = client.get("/healthz")

    assert response.status_code == 200
    body = response.json()
    assert body["status"] == "ok"
    assert body["checks"] == {"postgres": "ok", "redis": "ok"}


def test_create_accepts_and_enqueues(client, auth, enqueued):
    response = client.post("/v1/notifications", headers=auth, json=CREATED)

    assert response.status_code == 202
    body = response.json()
    assert body["status"] == "queued"
    assert body["attempts"] == 0
    assert body["max_attempts"] == 5
    assert body["delivered_at"] is None
    assert body["created_at"], "server defaults must be returned on insert"
    assert enqueued == [body["id"]], "the task must be published after the commit"

    with session_scope() as session:
        row = session.get(Notification, body["id"])
        assert row is not None
        assert row.status is Status.QUEUED


def test_create_persists_subject_and_payload(client, auth, enqueued):
    response = client.post(
        "/v1/notifications",
        headers=auth,
        json={**CREATED, "subject": "hi", "payload": {"ref": "abc", "n": 1}},
    )

    assert response.status_code == 202
    with session_scope() as session:
        row = session.get(Notification, response.json()["id"])
        assert row.subject == "hi"
        assert row.payload == {"ref": "abc", "n": 1}


def test_max_attempts_can_be_overridden(client, auth, enqueued):
    response = client.post("/v1/notifications", headers=auth, json={**CREATED, "max_attempts": 8})
    assert response.json()["max_attempts"] == 8


def test_requires_bearer_token(client):
    response = client.post("/v1/notifications", json=CREATED)

    assert response.status_code == 401
    assert "Bearer" in response.json()["detail"]


def test_rejects_unknown_api_key(client):
    response = client.post(
        "/v1/notifications", headers={"Authorization": "Bearer ntf_not-a-real-key"}, json=CREATED
    )
    assert response.status_code == 401


def test_rejects_webhook_recipient_that_is_not_a_url(client, auth):
    response = client.post(
        "/v1/notifications", headers=auth, json={**CREATED, "channel": "webhook", "recipient": "nope"}
    )
    assert response.status_code == 422


def test_rejects_email_without_at_sign(client, auth):
    response = client.post(
        "/v1/notifications", headers=auth, json={**CREATED, "channel": "email", "recipient": "nope"}
    )
    assert response.status_code == 422


def test_rejects_empty_body_and_unknown_channel(client, auth):
    assert client.post("/v1/notifications", headers=auth, json={**CREATED, "body": ""}).status_code == 422
    assert (
        client.post("/v1/notifications", headers=auth, json={**CREATED, "channel": "carrier-pigeon"}).status_code
        == 422
    )


def test_idempotency_key_replays_the_original(client, auth, enqueued):
    headers = {**auth, "Idempotency-Key": "order-42"}

    first = client.post("/v1/notifications", headers=headers, json=CREATED)
    second = client.post("/v1/notifications", headers=headers, json=CREATED)

    assert first.status_code == 202
    assert second.status_code == 200, "a replay is not a new resource"
    assert first.json()["id"] == second.json()["id"]
    assert len(enqueued) == 1, "a replay must not be enqueued twice"
    assert _count("WHERE idempotency_key = 'order-42'") == 1


def test_idempotency_race_returns_the_winner(client, auth, monkeypatch, tenant, enqueued):
    """The unique index decides a race; the loser returns the winner's row
    instead of a 500."""
    import app.routes as routes

    real_lookup = routes._by_idempotency_key
    calls = {"n": 0}

    def racing_lookup(session, tenant_id, key):
        calls["n"] += 1
        if calls["n"] == 1:
            with session_scope() as other:
                other.add(
                    Notification(
                        tenant_id=tenant.id,
                        channel=Channel.IN_APP,
                        recipient="winner",
                        body="I got there first",
                        payload={},
                        idempotency_key=key,
                        attempts=0,
                        max_attempts=5,
                    )
                )
            return None  # our own lookup saw nothing
        return real_lookup(session, tenant_id, key)

    monkeypatch.setattr(routes, "_by_idempotency_key", racing_lookup)

    response = client.post("/v1/notifications", headers={**auth, "Idempotency-Key": "race"}, json=CREATED)

    assert response.status_code == 200
    assert response.json()["recipient"] == "winner"
    assert _count("WHERE idempotency_key = 'race'") == 1
    assert enqueued == []


def test_get_returns_attempt_history(client, auth, enqueued):
    created = client.post("/v1/notifications", headers=auth, json=CREATED).json()

    response = client.get(f"/v1/notifications/{created['id']}", headers=auth)

    assert response.status_code == 200
    assert response.json()["attempts_log"] == []


def test_get_is_tenant_scoped(client, auth, api_key):
    import uuid

    from app.db import session_scope
    from app.models import Tenant
    from app.security import generate_api_key, generate_webhook_secret, hash_api_key

    other_key = generate_api_key()
    with session_scope() as session:
        session.add(
            Tenant(
                name="other",
                api_key_hash=hash_api_key(other_key),
                webhook_secret=generate_webhook_secret(),
            )
        )

    mine = client.post("/v1/notifications", headers=auth, json=CREATED).json()

    # Same id, different tenant: must look like it does not exist.
    assert client.get(f"/v1/notifications/{mine['id']}", headers={"Authorization": f"Bearer {other_key}"}).status_code == 404
    assert (
        client.get(f"/v1/notifications/{uuid.uuid4()}", headers=auth).status_code == 404
    )


def test_list_paginates_without_gaps_or_repeats(client, auth, enqueued):
    ids = [client.post("/v1/notifications", headers=auth, json=CREATED).json()["id"] for _ in range(5)]

    first = client.get("/v1/notifications", params={"limit": 2}, headers=auth).json()
    second = client.get(
        "/v1/notifications", params={"limit": 2, "cursor": first["next_cursor"]}, headers=auth
    ).json()
    third = client.get(
        "/v1/notifications", params={"limit": 2, "cursor": second["next_cursor"]}, headers=auth
    ).json()

    seen = [item["id"] for page in (first, second, third) for item in page["items"]]
    assert seen == list(reversed(ids)), "one page each, newest first, no repeats"
    assert third["next_cursor"] is None, "the last page has no cursor"


def test_list_filters_by_status_and_channel(client, auth, enqueued):
    client.post("/v1/notifications", headers=auth, json=CREATED)
    client.post("/v1/notifications", headers=auth, json={**CREATED, "channel": "email", "recipient": "a@b.c"})

    assert len(client.get("/v1/notifications", params={"status": "queued"}, headers=auth).json()["items"]) == 2
    assert (
        len(client.get("/v1/notifications", params={"channel": "in_app"}, headers=auth).json()["items"]) == 1
    )
    assert len(client.get("/v1/notifications", params={"status": "dead"}, headers=auth).json()["items"]) == 0


def test_list_rejects_a_bad_cursor(client, auth):
    assert client.get("/v1/notifications", params={"cursor": "not-base64"}, headers=auth).status_code == 400


def test_list_rejects_an_unknown_status(client, auth):
    assert client.get("/v1/notifications", params={"status": "exploded"}, headers=auth).status_code == 422


def test_stats_counts_by_status(client, auth, enqueued):
    for _ in range(3):
        client.post("/v1/notifications", headers=auth, json=CREATED)

    response = client.get("/v1/stats", params={"window_seconds": 3600}, headers=auth)

    assert response.status_code == 200
    body = response.json()
    assert body["total"] == 3
    assert body["by_status"] == {"queued": 3, "delivering": 0, "delivered": 0, "dead": 0}


def test_retry_tops_up_the_budget(client, auth, notification_factory, enqueued):
    dead = notification_factory(status=Status.DEAD, attempts=5)

    response = client.post(f"/v1/notifications/{dead.id}/retry", headers=auth)

    assert response.status_code == 202
    body = response.json()
    assert body["status"] == "queued"
    assert body["max_attempts"] == 10, "a manual retry grants a fresh budget"
    assert enqueued == [str(dead.id)]


def test_retry_refuses_a_notification_that_is_not_dead(client, auth, notification_factory):
    delivered = notification_factory(status=Status.DELIVERED, attempts=1)

    response = client.post(f"/v1/notifications/{delivered.id}/retry", headers=auth)

    assert response.status_code == 409
    assert "delivered" in response.json()["detail"]


def test_retry_unknown_id_is_404(client, auth):
    import uuid

    assert client.post(f"/v1/notifications/{uuid.uuid4()}/retry", headers=auth).status_code == 404


def test_metrics_endpoint_exposes_counters(client, auth, enqueued):
    client.post("/v1/notifications", headers=auth, json=CREATED)

    response = client.get("/metrics")

    assert response.status_code == 200
    assert "ntf_notifications_created_total" in response.text
    assert 'channel="in_app"' in response.text
    assert "ntf_http_requests_total" in response.text
    assert "notification_id" not in response.text, "no per-notification label cardinality"


def test_rate_limit_headers_on_success(client, auth, enqueued):
    response = client.post("/v1/notifications", headers=auth, json=CREATED)

    assert response.headers["x-ratelimit-limit"] == "600"
    assert int(response.headers["x-ratelimit-remaining"]) >= 0

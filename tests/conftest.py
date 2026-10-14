"""Test fixtures.

These run against real Postgres and Redis, not fakes: the interesting bugs in
this service live in transactions, unique indexes, Lua and Redis semantics, and
an in-memory stub would hide every one of them.

The database name and Redis index are rewritten to a scratch namespace, so a
test run can never touch the dev data or the Celery broker.
"""

import os
import re
from collections.abc import Iterator
from urllib.parse import urlsplit, urlunsplit

import pytest

# Must happen before anything imports app.config, which builds Settings once.
_RAW_DB = os.environ.get("DATABASE_URL", "postgresql+psycopg://ntf:ntf@localhost:5432/notifications")
TEST_DB_NAME = os.environ.get("TEST_DB_NAME", "notifications_test")


def _with_database(url: str, name: str) -> str:
    parts = urlsplit(url)
    return urlunsplit(parts._replace(path=f"/{name}"))


os.environ["DATABASE_URL"] = _with_database(_RAW_DB, TEST_DB_NAME)
# Index 15, not the broker's 0.
os.environ["REDIS_URL"] = re.sub(r"/\d+$", "/15", os.environ.get("REDIS_URL", "redis://localhost:6379/0"))
os.environ["CLICKHOUSE_ENABLED"] = "false"
os.environ["RATE_LIMIT_PER_MINUTE"] = "600"
os.environ["RATE_LIMIT_BURST"] = "60"
os.environ["RETRY_BASE_SECONDS"] = "2"
os.environ["RETRY_CAP_SECONDS"] = "300"

from fastapi.testclient import TestClient  # noqa: E402
from sqlalchemy import create_engine, text  # noqa: E402

from app.cache import get_redis  # noqa: E402
from app.config import settings  # noqa: E402
from app.db import get_engine  # noqa: E402
from app.main import app  # noqa: E402
from app.models import Base, Tenant  # noqa: E402
from app.security import generate_api_key, generate_webhook_secret, hash_api_key  # noqa: E402


def _ensure_test_database() -> None:
    admin = create_engine(
        _with_database(_RAW_DB, "postgres"), isolation_level="AUTOCOMMIT", poolclass=None
    )
    with admin.connect() as conn:
        exists = conn.execute(
            text("SELECT 1 FROM pg_database WHERE datname = :name"), {"name": TEST_DB_NAME}
        ).scalar()
        if not exists:
            conn.execute(text(f'CREATE DATABASE "{TEST_DB_NAME}"'))
    admin.dispose()


@pytest.fixture(scope="session", autouse=True)
def _database() -> Iterator[None]:
    _ensure_test_database()
    Base.metadata.create_all(get_engine())
    yield
    get_engine().dispose()


@pytest.fixture(autouse=True)
def clean_state() -> Iterator[None]:
    """Truncate, do not drop: the schema is created once for the session."""
    with get_engine().begin() as conn:
        conn.execute(
            text("TRUNCATE notifications, delivery_attempts, tenants RESTART IDENTITY CASCADE")
        )
    get_redis().flushdb()
    yield


@pytest.fixture
def client() -> Iterator[TestClient]:
    with TestClient(app) as test_client:
        yield test_client


@pytest.fixture
def api_key() -> str:
    return generate_api_key()


@pytest.fixture
def tenant(api_key: str) -> Tenant:
    from app.db import session_scope

    with session_scope() as session:
        row = Tenant(
            name="test-tenant",
            api_key_hash=hash_api_key(api_key),
            webhook_secret=generate_webhook_secret(),
            rate_limit_per_minute=None,
        )
        session.add(row)
    return row


@pytest.fixture
def auth(api_key: str, tenant: Tenant) -> dict[str, str]:
    return {"Authorization": f"Bearer {api_key}"}


@pytest.fixture
def enqueued(monkeypatch) -> list[str]:
    """Records task publishes instead of talking to a broker."""

    class FakeTask:
        def __init__(self) -> None:
            self.ids: list[str] = []

        def apply_async(self, args=None, **kwargs) -> None:
            self.ids.append(args[0])

    fake = FakeTask()
    monkeypatch.setattr("app.routes.deliver_notification", fake)
    monkeypatch.setattr("app.tasks.deliver_notification", fake)
    return fake.ids


@pytest.fixture
def notification_factory(tenant):
    """Creates a row directly, so delivery tests skip the HTTP layer."""
    from app.db import session_scope
    from app.models import Channel, Notification, Status

    def factory(
        *,
        channel: Channel = Channel.IN_APP,
        status: Status = Status.QUEUED,
        attempts: int = 0,
        max_attempts: int | None = None,
        recipient: str = "user-1",
        idempotency_key: str | None = None,
    ) -> Notification:
        with session_scope() as session:
            row = Notification(
                tenant_id=tenant.id,
                channel=channel,
                status=status,
                recipient=recipient,
                subject="subject",
                body="body",
                payload={},
                idempotency_key=idempotency_key,
                attempts=attempts,
                max_attempts=max_attempts or settings.max_attempts_default,
            )
            session.add(row)
        return row

    return factory

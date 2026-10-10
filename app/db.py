"""Engine and session plumbing. One sync engine, shared by API and worker."""

from collections.abc import Iterator
from contextlib import contextmanager

from sqlalchemy import Engine, create_engine
from sqlalchemy.orm import Session

from app.config import settings

_engine: Engine | None = None
_engine_url: str | None = None


def get_engine() -> Engine:
    """Pooled engine. Rebuilds if the URL changed, which is how tests point at
    a scratch database."""
    global _engine, _engine_url
    url = settings.database_url
    if _engine is None or _engine_url != url:
        if _engine is not None:
            _engine.dispose()
        _engine = create_engine(
            url,
            pool_pre_ping=True,
            pool_size=10,
            max_overflow=20,
            pool_recycle=1800,
        )
        _engine_url = url
    return _engine


# expire_on_commit=False: rows stay usable after a mid-task commit, so the
# worker can drop its transaction before making a network call and still read
# the fields it needs afterwards.
def _session() -> Session:
    return Session(get_engine(), expire_on_commit=False)


def get_session() -> Iterator[Session]:
    """FastAPI dependency."""
    with _session() as session:
        yield session


@contextmanager
def session_scope() -> Iterator[Session]:
    """Worker-side transaction: commit on success, roll back on any error."""
    with _session() as session:
        try:
            yield session
            session.commit()
        except Exception:
            session.rollback()
            raise

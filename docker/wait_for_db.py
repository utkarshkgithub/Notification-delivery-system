"""Block until Postgres accepts connections.

compose starts the API as soon as the postgres container is healthy, which is
not the same moment the database is ready for SQLAlchemy connections.
"""

import sys
import time

from sqlalchemy import create_engine, text

from app.config import settings

DEADLINE_SECONDS = 60


def main() -> int:
    engine = create_engine(settings.database_url, pool_pre_ping=True)
    deadline = time.monotonic() + DEADLINE_SECONDS
    while time.monotonic() < deadline:
        try:
            with engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            engine.dispose()
            return 0
        except Exception as exc:  # noqa: BLE001
            print(f"waiting for database: {exc.__class__.__name__}", flush=True)
            time.sleep(2)
    print("database did not become ready in time", file=sys.stderr)
    return 1


if __name__ == "__main__":
    raise SystemExit(main())

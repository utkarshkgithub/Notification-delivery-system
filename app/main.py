"""FastAPI entrypoint."""

import logging
import time
from contextlib import asynccontextmanager

import redis
from fastapi import FastAPI, Request
from prometheus_client import CONTENT_TYPE_LATEST, generate_latest
from sqlalchemy import text
from starlette.responses import JSONResponse, Response

from app import events
from app.cache import get_redis
from app.config import settings
from app.db import get_engine
from app.metrics import HTTP_DURATION, HTTP_REQUESTS, route_label
from app.routes import router

logging.basicConfig(
    level=settings.log_level.upper(),
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)
log = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(_app: FastAPI):
    try:
        events.ensure_schema()
        log.info("clickhouse event store ready")
    except Exception as exc:  # noqa: BLE001 - analytics is optional, the API is not
        log.warning("clickhouse schema not created at startup: %s", exc)
    yield


app = FastAPI(
    title="Notification Delivery Infrastructure",
    description=(
        "Multi-tenant notification delivery: accepts a request, queues it, "
        "retries with backoff, dead-letters what cannot be delivered."
    ),
    version="1.0.0",
    lifespan=lifespan,
)
app.include_router(router)


@app.middleware("http")
async def instrument(request: Request, call_next):
    started = time.perf_counter()
    response = await call_next(request)
    route = route_label(request.url.path)
    HTTP_REQUESTS.labels(
        method=request.method, route=route, status=str(response.status_code)
    ).inc()
    HTTP_DURATION.labels(method=request.method, route=route).observe(time.perf_counter() - started)
    return response


@app.get("/healthz", tags=["ops"])
def healthz() -> JSONResponse:
    """Liveness plus dependency check. ClickHouse is deliberately absent: it is
    an analytics sink, and losing it must not take the service out of a load
    balancer."""
    checks: dict[str, str] = {}
    try:
        with get_engine().connect() as conn:
            conn.execute(text("SELECT 1"))
        checks["postgres"] = "ok"
    except Exception as exc:  # noqa: BLE001
        checks["postgres"] = f"error: {exc}"
    try:
        get_redis().ping()
        checks["redis"] = "ok"
    except redis.RedisError as exc:
        checks["redis"] = f"error: {exc}"

    healthy = all(value == "ok" for value in checks.values())
    return JSONResponse(
        {"status": "ok" if healthy else "degraded", "checks": checks},
        status_code=200 if healthy else 503,
    )


@app.get("/metrics", tags=["ops"])
def metrics() -> Response:
    """Process-local metrics. Run with a single uvicorn worker unless
    PROMETHEUS_MULTIPROC_DIR is configured - see README."""
    return Response(content=generate_latest(), media_type=CONTENT_TYPE_LATEST)

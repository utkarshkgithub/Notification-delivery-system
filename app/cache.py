"""Redis client and cache helpers.

Every helper is fail-open: if Redis is down the API answers from Postgres
instead of returning 500. A cache outage must not become an outage.
"""

import json
import logging
import uuid
from typing import Any

import redis

from app.config import settings
from app.metrics import CACHE_EVENTS

log = logging.getLogger(__name__)

_client: redis.Redis | None = None
_client_url: str | None = None


def get_redis() -> redis.Redis:
    global _client, _client_url
    if _client is None or _client_url != settings.redis_url:
        if _client is not None:
            _client.close()
        _client = redis.Redis.from_url(
            settings.redis_url,
            decode_responses=True,
            socket_timeout=2.0,
            socket_connect_timeout=2.0,
            health_check_interval=30,
        )
        _client_url = settings.redis_url
    return _client


def notification_key(notification_id: uuid.UUID | str) -> str:
    return f"ntf:v1:{notification_id}"


def cache_get(key: str) -> Any | None:
    try:
        raw = get_redis().get(key)
    except redis.RedisError as exc:
        log.warning("cache read failed for %s: %s", key, exc)
        return None
    if raw is None:
        CACHE_EVENTS.labels(result="miss").inc()
        return None
    CACHE_EVENTS.labels(result="hit").inc()
    try:
        return json.loads(raw)
    except json.JSONDecodeError:
        # Corrupt value: drop it rather than serving garbage.
        cache_delete(key)
        return None


def cache_set(key: str, value: Any, ttl_seconds: int) -> None:
    try:
        get_redis().set(key, json.dumps(value, default=str), ex=ttl_seconds)
    except redis.RedisError as exc:
        log.warning("cache write failed for %s: %s", key, exc)


def cache_delete(*keys: str) -> None:
    if not keys:
        return
    try:
        get_redis().delete(*keys)
    except redis.RedisError as exc:
        log.warning("cache delete failed: %s", exc)

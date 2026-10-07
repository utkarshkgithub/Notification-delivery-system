"""Per-tenant token bucket, evaluated inside Redis.

A fixed-window counter would be one INCR, but it lets a tenant send 2x the
limit across a window boundary. The bucket costs one round trip and is smooth,
and running it as a Lua script keeps it atomic - no read-modify-write race
between concurrent requests.
"""

import logging
import time

import redis

from app.cache import get_redis
from app.config import settings
from app.metrics import RATE_LIMIT_BACKEND_ERRORS, RATE_LIMIT_REJECTED

log = logging.getLogger(__name__)

# KEYS[1] bucket key
# ARGV[1] capacity (burst)   ARGV[2] refill per second   ARGV[3] now in ms
# ARGV[4] cost
# -> {allowed, remaining, retry_after_ms}
TOKEN_BUCKET = """
local key = KEYS[1]
local capacity = tonumber(ARGV[1])
local refill = tonumber(ARGV[2])
local now = tonumber(ARGV[3])
local cost = tonumber(ARGV[4])

local bucket = redis.call('HMGET', key, 'tokens', 'ts')
local tokens = tonumber(bucket[1])
local ts = tonumber(bucket[2])
if tokens == nil then
  tokens = capacity
  ts = now
end

tokens = math.min(capacity, tokens + math.max(0, now - ts) / 1000 * refill)

local allowed = 0
local retry_ms = 0
if tokens >= cost then
  allowed = 1
  tokens = tokens - cost
else
  retry_ms = math.ceil((cost - tokens) / refill * 1000)
end

redis.call('HSET', key, 'tokens', tokens, 'ts', now)
redis.call('PEXPIRE', key, math.ceil(capacity / refill * 1000) + 1000)
return {allowed, math.floor(tokens), retry_ms}
"""

_script = None
_script_url: str | None = None


def _bucket_script():
    """register_script gives us EVALSHA with an automatic NOSCRIPT retry."""
    global _script, _script_url
    if _script is None or _script_url != settings.redis_url:
        _script = get_redis().register_script(TOKEN_BUCKET)
        _script_url = settings.redis_url
    return _script


def check_rate_limit(
    tenant_id, limit_per_minute: int, burst: int, cost: int = 1
) -> tuple[bool, int, int]:
    """Returns (allowed, remaining, retry_after_seconds).

    Fails open on a Redis error: dropping customer notifications because the
    limiter is unreachable is worse than briefly under-limiting.
    """
    refill = max(limit_per_minute, 1) / 60.0
    try:
        allowed, remaining, retry_ms = _bucket_script()(
            keys=[f"rl:{tenant_id}"],
            args=[burst, refill, int(time.time() * 1000), cost],
        )
    except redis.RedisError as exc:
        log.warning("rate limiter unavailable, failing open: %s", exc)
        RATE_LIMIT_BACKEND_ERRORS.inc()
        return True, burst, 0

    if not allowed:
        RATE_LIMIT_REJECTED.inc()
    return bool(allowed), int(remaining), int((retry_ms + 999) // 1000)

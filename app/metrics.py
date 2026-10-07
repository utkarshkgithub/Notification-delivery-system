"""Prometheus metrics.

Label discipline: channel, outcome, method, route and status only. Never
tenant_id or notification_id - unbounded label values turn a metric into a
memory leak and a scrape into a timeout.
"""

import re
import uuid

from prometheus_client import Counter, Histogram

NOTIFICATIONS_CREATED = Counter(
    "ntf_notifications_created_total", "Notifications accepted by the API", ["channel"]
)
DELIVERY_ATTEMPTS = Counter(
    "ntf_delivery_attempts_total", "Delivery attempts by result", ["channel", "outcome"]
)
DELIVERY_DURATION = Histogram(
    "ntf_delivery_duration_seconds",
    "Time inside the channel sender",
    ["channel"],
    buckets=(0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0),
)
RETRIES_SCHEDULED = Counter(
    "ntf_delivery_retries_scheduled_total", "Retries handed back to the queue", ["channel"]
)
DEAD_LETTERED = Counter(
    "ntf_notifications_dead_total", "Notifications that exhausted their attempts", ["channel"]
)
STUCK_RECLAIMED = Counter(
    "ntf_stuck_deliveries_reclaimed_total", "Deliveries re-queued after a worker died mid-flight"
)
RATE_LIMIT_REJECTED = Counter(
    "ntf_ratelimit_rejected_total", "Requests rejected with 429"
)
RATE_LIMIT_BACKEND_ERRORS = Counter(
    "ntf_ratelimit_backend_errors_total", "Rate limit checks that fell back to allowed"
)
CACHE_EVENTS = Counter(
    "ntf_cache_events_total", "Response cache lookups", ["result"]  # hit | miss
)
CLICKHOUSE_WRITE_ERRORS = Counter(
    "ntf_clickhouse_write_errors_total", "Delivery events dropped by the analytics sink"
)
HTTP_REQUESTS = Counter(
    "ntf_http_requests_total", "HTTP requests", ["method", "route", "status"]
)
HTTP_DURATION = Histogram(
    "ntf_http_request_duration_seconds",
    "HTTP request latency",
    ["method", "route"],
    buckets=(0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5),
)

_UUID = re.compile(r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}", re.I)


def route_label(path: str) -> str:
    """Collapse path params so /v1/notifications/<uuid> is one label value.

    Starlette only fills scope['route'] after routing, which is too late for a
    plain middleware, so normalise the raw path instead.
    """
    return _UUID.sub("{id}", path)


def is_uuid(value: str) -> bool:
    try:
        uuid.UUID(value)
    except (ValueError, AttributeError, TypeError):
        return False
    return True

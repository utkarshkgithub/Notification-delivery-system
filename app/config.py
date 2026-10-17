"""Single source of truth for every tunable in the service."""

from functools import lru_cache

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    # Postgres + Redis + ClickHouse. Web process, worker and Alembic share one
    # sync SQLAlchemy engine: Celery is sync anyway and a second async driver
    # would just be another thing to keep in sync.
    database_url: str = "postgresql+psycopg://ntf:ntf@localhost:5432/notifications"
    redis_url: str = "redis://localhost:6379/0"
    clickhouse_url: str = "http://localhost:8123"
    clickhouse_database: str = "notifications"
    clickhouse_user: str = "default"
    clickhouse_password: str = ""
    clickhouse_enabled: bool = True

    # Delivery policy.
    max_attempts_default: int = 5
    retry_base_seconds: float = 2.0
    retry_cap_seconds: float = 300.0
    webhook_timeout_seconds: float = 5.0
    stuck_delivery_timeout_seconds: int = 300
    reclaimer_interval_seconds: int = 60
    # How many messages a worker keeps in flight per thread. Checked by
    # measurement, not by taste: see the benchmark table in the README.
    worker_prefetch_multiplier: int = 8
    # A queued notification that is this far past its due time was accepted but
    # never picked up (broker flushed, message dropped); the sweeper re-publishes it.
    queued_sweep_grace_seconds: int = 120
    sweeper_interval_seconds: int = 60

    # Rate limiting, per tenant, unless the tenant row overrides it.
    rate_limit_per_minute: int = 600
    rate_limit_burst: int = 60

    # Caching.
    notification_cache_ttl_seconds: int = 5
    tenant_cache_ttl_seconds: int = 60

    # Email. Empty host means "log it instead of sending".
    smtp_host: str = ""
    smtp_port: int = 587
    smtp_user: str = ""
    smtp_password: str = ""
    smtp_from: str = "notifications@example.com"
    smtp_starttls: bool = True

    # Observability.
    metrics_port: int = 9101
    log_level: str = "INFO"


@lru_cache
def get_settings() -> Settings:
    return Settings()


settings = get_settings()

"""Wire format. Kept separate from models.py so the schema never leaks out."""

import uuid
from datetime import datetime

from pydantic import BaseModel, ConfigDict, Field, HttpUrl, field_validator

from app.models import Channel, Outcome, Status


class NotificationCreate(BaseModel):
    channel: Channel
    recipient: str = Field(min_length=1, max_length=512)
    body: str = Field(min_length=1, max_length=20_000)
    subject: str | None = Field(default=None, max_length=255)
    payload: dict = Field(default_factory=dict)
    max_attempts: int | None = Field(default=None, ge=1, le=10)

    @field_validator("recipient")
    @classmethod
    def _recipient_shape(cls, v: str, info) -> str:
        """Fail a doomed request at the edge instead of burning 5 retries on it."""
        channel = info.data.get("channel")
        if channel is Channel.WEBHOOK:
            if not v.startswith(("http://", "https://")):
                raise ValueError("webhook recipient must be an http(s) URL")
            HttpUrl(v)  # raises on anything malformed
        if channel is Channel.EMAIL and "@" not in v:
            raise ValueError("email recipient must contain @")
        return v


class AttemptOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    attempt: int
    outcome: Outcome
    status_code: int | None
    error: str | None
    duration_ms: int
    created_at: datetime


class NotificationOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: uuid.UUID
    channel: Channel
    status: Status
    recipient: str
    subject: str | None
    attempts: int
    max_attempts: int
    next_attempt_at: datetime
    last_error: str | None
    delivered_at: datetime | None
    created_at: datetime
    updated_at: datetime


class NotificationDetail(NotificationOut):
    body: str
    payload: dict
    attempts_log: list[AttemptOut] = []


class Page(BaseModel):
    items: list[NotificationOut]
    next_cursor: str | None


class StatusCounts(BaseModel):
    queued: int
    delivering: int
    delivered: int
    dead: int


class StatsOut(BaseModel):
    window_seconds: int
    total: int
    by_status: StatusCounts


class EventCount(BaseModel):
    event: str
    total: int
    avg_duration_ms: float
    last_seen: datetime | None


class EventStatsOut(BaseModel):
    window_seconds: int
    events: list[EventCount]

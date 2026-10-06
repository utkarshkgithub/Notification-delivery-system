"""Relational schema: who sends, what they sent, and every attempt at it."""

import enum
import uuid
from datetime import datetime

from sqlalchemy import (
    BigInteger,
    DateTime,
    Enum,
    ForeignKey,
    Index,
    Integer,
    SmallInteger,
    String,
    Text,
    UniqueConstraint,
    desc,
    func,
)
from sqlalchemy.dialects.postgresql import JSONB, UUID
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship


class Base(DeclarativeBase):
    pass


class Channel(str, enum.Enum):
    EMAIL = "email"
    WEBHOOK = "webhook"
    IN_APP = "in_app"


class Status(str, enum.Enum):
    QUEUED = "queued"
    DELIVERING = "delivering"
    DELIVERED = "delivered"
    DEAD = "dead"


class Outcome(str, enum.Enum):
    DELIVERED = "delivered"
    FAILED = "failed"  # retryable, will be tried again
    PERMANENT = "permanent"  # 4xx / bad payload, never retried


def _enum(enum_cls: type[enum.Enum], name: str) -> Enum:
    """VARCHAR + CHECK instead of a native pg enum: adding a value is then a
    one-line migration rather than ALTER TYPE gymnastics."""
    return Enum(
        enum_cls,
        name=name,
        native_enum=False,
        length=20,
        values_callable=lambda e: [m.value for m in e],
    )


TIMESTAMP = DateTime(timezone=True)


class Tenant(Base):
    __tablename__ = "tenants"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    name: Mapped[str] = mapped_column(String(120), unique=True)
    # Only the SHA-256 of the key is stored; the plaintext is shown once at
    # creation time. Same idea as a password hash, no bcrypt cost needed
    # because the key is 256 bits of random, not a human password.
    api_key_hash: Mapped[str] = mapped_column(String(64), unique=True)
    webhook_secret: Mapped[str] = mapped_column(String(64))
    rate_limit_per_minute: Mapped[int | None] = mapped_column(Integer, nullable=True)
    created_at: Mapped[datetime] = mapped_column(TIMESTAMP, server_default=func.now())

    notifications: Mapped[list["Notification"]] = relationship(back_populates="tenant")


class Notification(Base):
    __tablename__ = "notifications"
    __table_args__ = (
        # The reclaimer and the retry path both scan by status + due time.
        Index("ix_notifications_due", "status", "next_attempt_at"),
        # Keyset pagination for GET /v1/notifications.
        Index("ix_notifications_tenant_created", "tenant_id", desc("created_at"), desc("id")),
        # Status breakdown for GET /v1/stats.
        Index("ix_notifications_tenant_status", "tenant_id", "status"),
        UniqueConstraint("tenant_id", "idempotency_key", name="uq_notifications_tenant_idem"),
    )

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    tenant_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("tenants.id", ondelete="CASCADE"), index=True
    )
    channel: Mapped[Channel] = mapped_column(_enum(Channel, "notification_channel"))
    status: Mapped[Status] = mapped_column(
        _enum(Status, "notification_status"), default=Status.QUEUED
    )
    # Debateable name, but for every channel it is "who or what receives this":
    # an address, a webhook URL, or an end-user id for the in-app feed.
    recipient: Mapped[str] = mapped_column(String(512))
    subject: Mapped[str | None] = mapped_column(String(255), nullable=True)
    body: Mapped[str] = mapped_column(Text)
    payload: Mapped[dict] = mapped_column(JSONB, default=dict)
    idempotency_key: Mapped[str | None] = mapped_column(String(255), nullable=True)
    attempts: Mapped[int] = mapped_column(SmallInteger, default=0)
    max_attempts: Mapped[int] = mapped_column(SmallInteger)
    next_attempt_at: Mapped[datetime] = mapped_column(TIMESTAMP, server_default=func.now())
    last_error: Mapped[str | None] = mapped_column(Text, nullable=True)
    last_status_code: Mapped[int | None] = mapped_column(SmallInteger, nullable=True)
    delivered_at: Mapped[datetime | None] = mapped_column(TIMESTAMP, nullable=True)
    created_at: Mapped[datetime] = mapped_column(TIMESTAMP, server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(
        TIMESTAMP, server_default=func.now(), onupdate=func.now()
    )

    tenant: Mapped[Tenant] = relationship(back_populates="notifications")
    attempts_log: Mapped[list["DeliveryAttempt"]] = relationship(
        back_populates="notification", cascade="all, delete-orphan"
    )


class DeliveryAttempt(Base):
    __tablename__ = "delivery_attempts"
    __table_args__ = (Index("ix_attempts_notification_attempt", "notification_id", "attempt"),)

    # bigserial: this table grows one row per try, so it wants a narrow key.
    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    notification_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("notifications.id", ondelete="CASCADE")
    )
    attempt: Mapped[int] = mapped_column(SmallInteger)
    outcome: Mapped[Outcome] = mapped_column(_enum(Outcome, "delivery_outcome"))
    status_code: Mapped[int | None] = mapped_column(SmallInteger, nullable=True)
    error: Mapped[str | None] = mapped_column(Text, nullable=True)
    duration_ms: Mapped[int] = mapped_column(Integer, default=0)
    created_at: Mapped[datetime] = mapped_column(TIMESTAMP, server_default=func.now())

    notification: Mapped[Notification] = relationship(back_populates="attempts_log")

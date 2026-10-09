"""Channel senders.

Each sender answers one question: was this worth retrying? A 4xx from a
receiver, or an SMTP user-unknown, will fail identically five times, so it
raises PermanentFailure and the worker stops immediately instead of pretending
to be busy. Everything else is a DeliveryResult(ok=False) and gets retried.
"""

import json
import logging
import smtplib
import time
from dataclasses import dataclass
from email.message import EmailMessage

import httpx

from app.config import settings
from app.models import Channel, Notification, Tenant
from app.security import SIGNATURE_HEADER, TIMESTAMP_HEADER, sign_payload

log = logging.getLogger(__name__)

# 408 and 429 are "not now", not "no" - those still get retried.
RETRYABLE_STATUS = {408, 429}


class PermanentFailure(Exception):
    """The receiver rejected this on its merits. Retrying cannot help."""

    def __init__(self, message: str, status_code: int | None = None, duration_ms: int = 0):
        super().__init__(message)
        self.status_code = status_code
        self.duration_ms = duration_ms


@dataclass
class DeliveryResult:
    ok: bool
    status_code: int | None = None
    error: str | None = None
    duration_ms: int = 0


def deliver(notification: Notification, tenant: Tenant) -> DeliveryResult:
    match notification.channel:
        case Channel.EMAIL:
            return _send_email(notification)
        case Channel.WEBHOOK:
            return _send_webhook(notification, tenant)
        case Channel.IN_APP:
            return _store_in_app(notification)
    raise PermanentFailure(f"unknown channel {notification.channel!r}")


def _send_webhook(notification: Notification, tenant: Tenant) -> DeliveryResult:
    body = json.dumps(
        {
            "id": str(notification.id),
            "channel": notification.channel.value,
            "subject": notification.subject,
            "body": notification.body,
            "payload": notification.payload,
            "created_at": notification.created_at.isoformat(),
        },
        separators=(",", ":"),
    ).encode()
    timestamp = str(int(time.time()))
    headers = {
        "Content-Type": "application/json",
        "X-Notification-Id": str(notification.id),
        TIMESTAMP_HEADER: timestamp,
        SIGNATURE_HEADER: f"sha256={sign_payload(tenant.webhook_secret, timestamp, body)}",
    }

    started = time.perf_counter()
    try:
        response = httpx.post(
            notification.recipient,
            content=body,
            headers=headers,
            timeout=settings.webhook_timeout_seconds,
        )
    except httpx.HTTPError as exc:
        return DeliveryResult(
            ok=False,
            error=f"transport error: {exc.__class__.__name__}: {exc}",
            duration_ms=_elapsed_ms(started),
        )

    duration_ms = _elapsed_ms(started)
    status = response.status_code
    if 200 <= status < 300:
        return DeliveryResult(ok=True, status_code=status, duration_ms=duration_ms)
    if status in RETRYABLE_STATUS or status >= 500:
        return DeliveryResult(
            ok=False,
            status_code=status,
            error=f"receiver returned {status}",
            duration_ms=duration_ms,
        )
    raise PermanentFailure(
        f"receiver rejected the delivery with {status}", status_code=status, duration_ms=duration_ms
    )


def _send_email(notification: Notification) -> DeliveryResult:
    """Console transport when SMTP_HOST is unset, which is what dev and CI use."""
    if not settings.smtp_host:
        log.info("[console-email] to=%s subject=%s", notification.recipient, notification.subject)
        return DeliveryResult(ok=True, error="console transport (no SMTP_HOST configured)")

    message = EmailMessage()
    message["From"] = settings.smtp_from
    message["To"] = notification.recipient
    message["Subject"] = notification.subject or "(no subject)"
    message.set_content(notification.body)

    started = time.perf_counter()
    try:
        with smtplib.SMTP(settings.smtp_host, settings.smtp_port, timeout=10) as smtp:
            if settings.smtp_starttls:
                smtp.starttls()
            if settings.smtp_user:
                smtp.login(settings.smtp_user, settings.smtp_password)
            smtp.send_message(message)
    except smtplib.SMTPRecipientsRefused as exc:
        raise PermanentFailure(f"recipient refused: {exc}") from exc
    except (smtplib.SMTPException, OSError) as exc:
        return DeliveryResult(
            ok=False, error=f"smtp error: {exc}", duration_ms=_elapsed_ms(started)
        )
    return DeliveryResult(ok=True, duration_ms=_elapsed_ms(started))


def _store_in_app(notification: Notification) -> DeliveryResult:
    """Nothing to send: the recipient polls the API for its own feed, so the
    row reaching 'delivered' is the delivery."""
    return DeliveryResult(ok=True)


def _elapsed_ms(started: float) -> int:
    return int((time.perf_counter() - started) * 1000)

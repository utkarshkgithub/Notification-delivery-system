"""Webhook signing and channel failure classification.

The sender is tested against the verifier it ships, so the two cannot drift
apart.
"""

import json
import smtplib
import time

import httpx
import pytest

from app.channels import PermanentFailure, deliver
from app.config import settings
from app.models import Channel
from app.security import SIGNATURE_HEADER, TIMESTAMP_HEADER, sign_payload, verify_signature

SECRET = "s" * 64


def test_signature_round_trip():
    body = b'{"hello":"world"}'
    timestamp = str(int(time.time()))

    signature = sign_payload(SECRET, timestamp, body)

    assert verify_signature(SECRET, timestamp, body, signature) is True
    assert verify_signature(SECRET, timestamp, body, f"sha256={signature}") is True


def test_tampered_body_fails():
    timestamp = str(int(time.time()))
    signature = sign_payload(SECRET, timestamp, b"original")

    assert verify_signature(SECRET, timestamp, b"tampered", signature) is False


def test_wrong_secret_fails():
    body = b"payload"
    timestamp = str(int(time.time()))

    assert verify_signature("other-secret", timestamp, body, sign_payload(SECRET, timestamp, body)) is False


def test_replay_outside_the_tolerance_fails():
    body = b"payload"
    stale = str(int(time.time()) - 400)
    signature = sign_payload(SECRET, stale, body)

    assert verify_signature(SECRET, stale, body, signature, tolerance=300) is False
    assert verify_signature(SECRET, stale, body, signature, tolerance=3600) is True


def test_malformed_timestamp_or_signature_fails():
    body = b"payload"

    assert verify_signature(SECRET, "not-a-number", body, "abc") is False
    assert verify_signature(SECRET, str(int(time.time())), body, "") is False


def test_sender_signs_what_the_verifier_accepts(monkeypatch, notification_factory, tenant):
    captured: dict = {}

    def fake_post(url, content=None, headers=None, timeout=None):
        captured.update(url=url, body=content, headers=headers)
        return httpx.Response(200, request=httpx.Request("POST", url))

    monkeypatch.setattr("app.channels.httpx.post", fake_post)
    notification = notification_factory(channel=Channel.WEBHOOK, recipient="https://receiver.test/hook")

    result = deliver(notification, tenant)

    assert result.ok is True
    assert result.status_code == 200
    assert captured["url"] == "https://receiver.test/hook"
    assert json.loads(captured["body"])["body"] == "body"
    assert captured["headers"]["X-Notification-Id"] == str(notification.id)
    assert (
        verify_signature(
            tenant.webhook_secret,
            captured["headers"][TIMESTAMP_HEADER],
            captured["body"],
            captured["headers"][SIGNATURE_HEADER],
        )
        is True
    )


@pytest.mark.parametrize("status_code", [500, 502, 503, 408, 429])
def test_transient_statuses_are_retryable(status_code, monkeypatch, notification_factory, tenant):
    monkeypatch.setattr(
        "app.channels.httpx.post",
        lambda url, **kwargs: httpx.Response(status_code, request=httpx.Request("POST", url)),
    )
    notification = notification_factory(channel=Channel.WEBHOOK, recipient="https://receiver.test/hook")

    result = deliver(notification, tenant)

    assert result.ok is False
    assert result.status_code == status_code


@pytest.mark.parametrize("status_code", [400, 401, 403, 404, 410, 422])
def test_client_errors_are_permanent(status_code, monkeypatch, notification_factory, tenant):
    monkeypatch.setattr(
        "app.channels.httpx.post",
        lambda url, **kwargs: httpx.Response(status_code, request=httpx.Request("POST", url)),
    )
    notification = notification_factory(channel=Channel.WEBHOOK, recipient="https://receiver.test/hook")

    with pytest.raises(PermanentFailure) as excinfo:
        deliver(notification, tenant)

    assert excinfo.value.status_code == status_code


def test_redirect_is_permanent_because_we_do_not_follow(monkeypatch, notification_factory, tenant):
    """httpx does not follow redirects here, so a 3xx is a misconfigured
    receiver, not a transient problem. Retrying it five times would only hide
    the misconfiguration."""
    monkeypatch.setattr(
        "app.channels.httpx.post",
        lambda url, **kwargs: httpx.Response(302, request=httpx.Request("POST", url)),
    )
    notification = notification_factory(channel=Channel.WEBHOOK, recipient="https://receiver.test/hook")

    with pytest.raises(PermanentFailure):
        deliver(notification, tenant)


def test_transport_error_is_retryable(monkeypatch, notification_factory, tenant):
    def boom(url, **kwargs):
        raise httpx.ConnectTimeout("timed out")

    monkeypatch.setattr("app.channels.httpx.post", boom)
    notification = notification_factory(channel=Channel.WEBHOOK, recipient="https://receiver.test/hook")

    result = deliver(notification, tenant)

    assert result.ok is False
    assert "transport error" in result.error
    assert result.duration_ms >= 0


def test_email_uses_the_console_transport_without_smtp(monkeypatch, notification_factory, tenant):
    monkeypatch.setattr(settings, "smtp_host", "")
    notification = notification_factory(channel=Channel.EMAIL, recipient="someone@example.com")

    result = deliver(notification, tenant)

    assert result.ok is True
    assert "console transport" in result.error


def test_unknown_recipient_is_permanent(monkeypatch, notification_factory, tenant):
    monkeypatch.setattr(settings, "smtp_host", "smtp.example.com")

    class FakeSMTP:
        def __init__(self, *args, **kwargs):
            pass

        def __enter__(self):
            return self

        def __exit__(self, *args):
            return False

        def starttls(self):
            pass

        def send_message(self, message):
            raise smtplib.SMTPRecipientsRefused({"someone@example.com": (550, b"no such user")})

    monkeypatch.setattr("app.channels.smtplib.SMTP", FakeSMTP)
    notification = notification_factory(channel=Channel.EMAIL, recipient="someone@example.com")

    with pytest.raises(PermanentFailure):
        deliver(notification, tenant)


def test_smtp_outage_is_retryable(monkeypatch, notification_factory, tenant):
    monkeypatch.setattr(settings, "smtp_host", "smtp.example.com")

    class FakeSMTP:
        def __init__(self, *args, **kwargs):
            pass

        def __enter__(self):
            return self

        def __exit__(self, *args):
            return False

        def starttls(self):
            pass

        def send_message(self, message):
            raise smtplib.SMTPServerDisconnected("gone")

    monkeypatch.setattr("app.channels.smtplib.SMTP", FakeSMTP)
    notification = notification_factory(channel=Channel.EMAIL, recipient="someone@example.com")

    result = deliver(notification, tenant)

    assert result.ok is False
    assert "smtp error" in result.error


def test_in_app_needs_no_transport(notification_factory, tenant):
    notification = notification_factory(channel=Channel.IN_APP)

    assert deliver(notification, tenant).ok is True

"""API key handling and webhook signatures."""

import hashlib
import hmac
import secrets
import time

API_KEY_PREFIX = "ntf_"
SIGNATURE_HEADER = "X-Webhook-Signature"
TIMESTAMP_HEADER = "X-Webhook-Timestamp"
DEFAULT_TOLERANCE_SECONDS = 300


def hash_api_key(api_key: str) -> str:
    """Keys are 256 bits of random, so a single SHA-256 pass is enough - there
    is nothing to brute-force. Lookup is by this hash, which is why it is
    indexed and unique."""
    return hashlib.sha256(api_key.encode()).hexdigest()


def generate_api_key() -> str:
    return API_KEY_PREFIX + secrets.token_urlsafe(32)


def generate_webhook_secret() -> str:
    return secrets.token_hex(32)


def sign_payload(secret: str, timestamp: str, body: bytes) -> str:
    """HMAC over `<timestamp>.<body>`. The timestamp is inside the signature so
    a captured request cannot be replayed later with a fresh header."""
    mac = hmac.new(secret.encode(), timestamp.encode() + b"." + body, hashlib.sha256)
    return mac.hexdigest()


def verify_signature(
    secret: str,
    timestamp: str,
    body: bytes,
    signature: str,
    tolerance: int = DEFAULT_TOLERANCE_SECONDS,
    now: float | None = None,
) -> bool:
    """What a receiver runs. Shipped here so the test suite and the demo
    receiver verify the real thing instead of a copy of it."""
    try:
        sent_at = int(timestamp)
    except (TypeError, ValueError):
        return False
    if abs((now if now is not None else time.time()) - sent_at) > tolerance:
        return False
    expected = sign_payload(secret, timestamp, body)
    candidate = signature.removeprefix("sha256=")
    return hmac.compare_digest(expected, candidate)

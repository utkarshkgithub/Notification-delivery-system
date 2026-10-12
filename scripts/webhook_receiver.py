"""A stand-in for the customer's server.

Verifies signatures exactly the way the real receiver would, counts what it
got, and can be told to fail so the retry ladder and the dead-letter path are
demonstrable without editing code:

    python scripts/webhook_receiver.py --fail-first 2    # 503 twice, then 200
    python scripts/webhook_receiver.py --always 400      # permanent rejection

GET /_stats returns the tally, which is what the load test asserts against.
"""

import argparse
import json
import logging
import os
import threading
from collections import Counter, defaultdict
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

from app.security import SIGNATURE_HEADER, TIMESTAMP_HEADER, verify_signature

log = logging.getLogger("receiver")

STATE = Counter()
ATTEMPTS_PER_NOTIFICATION: dict[str, int] = defaultdict(int)
LOCK = threading.Lock()

SECRET: str = ""
FAIL_FIRST: int = 0
ALWAYS_STATUS: int = 0


class Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def do_GET(self) -> None:  # noqa: N802
        if self.path.rstrip("/") == "/_stats":
            self._json(200, dict(STATE))
            return
        if self.path.rstrip("/") == "/healthz":
            self._json(200, {"status": "ok"})
            return
        self._json(404, {"error": "not found"})

    def do_POST(self) -> None:  # noqa: N802
        length = int(self.headers.get("Content-Length") or 0)
        body = self.rfile.read(length)
        notification_id = self.headers.get("X-Notification-Id", "unknown")

        with LOCK:
            STATE["received"] += 1
            ATTEMPTS_PER_NOTIFICATION[notification_id] += 1
            attempt = ATTEMPTS_PER_NOTIFICATION[notification_id]

        if SECRET:
            signature = self.headers.get(SIGNATURE_HEADER, "")
            timestamp = self.headers.get(TIMESTAMP_HEADER, "")
            if not verify_signature(SECRET, timestamp, body, signature):
                with LOCK:
                    STATE["rejected_signature"] += 1
                log.warning("id=%s attempt=%s BAD SIGNATURE", notification_id, attempt)
                self._json(401, {"error": "invalid signature"})
                return
            with LOCK:
                STATE["verified_signature"] += 1

        if ALWAYS_STATUS:
            with LOCK:
                STATE[f"returned_{ALWAYS_STATUS}"] += 1
            log.info("id=%s attempt=%s -> %s (injected)", notification_id, attempt, ALWAYS_STATUS)
            self._json(ALWAYS_STATUS, {"error": "injected failure"})
            return

        if attempt <= FAIL_FIRST:
            with LOCK:
                STATE["returned_503"] += 1
            log.info("id=%s attempt=%s -> 503 (injected)", notification_id, attempt)
            self._json(503, {"error": "try again"})
            return

        with LOCK:
            STATE["accepted"] += 1
        log.info("id=%s attempt=%s -> 200", notification_id, attempt)
        self._json(200, {"ok": True})

    def _json(self, status: int, payload: dict) -> None:
        raw = json.dumps(payload).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(raw)))
        self.end_headers()
        self.wfile.write(raw)

    def log_message(self, fmt: str, *args) -> None:
        pass  # the handler logs deliberately; the default line is noise


def main() -> int:
    global SECRET, FAIL_FIRST, ALWAYS_STATUS
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--port", type=int, default=8090)
    parser.add_argument("--host", default="0.0.0.0")
    parser.add_argument("--secret", default=os.environ.get("WEBHOOK_SECRET", ""))
    parser.add_argument("--fail-first", type=int, default=0)
    parser.add_argument("--always", type=int, default=0)
    args = parser.parse_args()

    SECRET, FAIL_FIRST, ALWAYS_STATUS = args.secret, args.fail_first, args.always
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")
    if not SECRET:
        log.warning("no WEBHOOK_SECRET set - signatures will not be verified")

    server = ThreadingHTTPServer((args.host, args.port), Handler)
    log.info(
        "receiver on %s:%s | fail_first=%s always=%s",
        args.host,
        args.port,
        FAIL_FIRST,
        ALWAYS_STATUS or "-",
    )
    server.serve_forever()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

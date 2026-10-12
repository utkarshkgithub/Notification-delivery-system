"""Load test.

Two phases, because they measure different things:
  1. ingest   - POST /v1/notifications from N threads: requests/sec and latency
                percentiles for the API itself.
  2. delivery - poll /v1/stats until nothing is queued: end-to-end drain time
                and deliveries/sec through Redis, Celery and the receiver.

    python scripts/loadtest.py --api-key ntf_... --count 2000 --concurrency 16

Writes results/loadtest.json next to a printed table.
"""

import argparse
import json
import statistics
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from pathlib import Path

import httpx

TERMINAL = ("delivered", "dead")


def percentile(values: list[float], fraction: float) -> float:
    if not values:
        return 0.0
    ordered = sorted(values)
    index = min(len(ordered) - 1, int(len(ordered) * fraction))
    return ordered[index]


def ingest(client: httpx.Client, api_key: str, count: int, concurrency: int, recipient: str, channel: str):
    headers = {"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"}
    latencies: list[float] = []
    errors: list[str] = []
    lock = threading.Lock()

    def one(index: int) -> None:
        payload = {
            "channel": channel,
            "recipient": recipient,
            "subject": f"load test {index}",
            "body": f"message {index}",
        }
        started = time.perf_counter()
        try:
            response = client.post(
                "/v1/notifications", json=payload, headers=headers, timeout=30.0
            )
            elapsed = (time.perf_counter() - started) * 1000
            with lock:
                latencies.append(elapsed)
                if response.status_code >= 400:
                    errors.append(f"{response.status_code}: {response.text[:120]}")
        except httpx.HTTPError as exc:
            with lock:
                errors.append(f"{exc.__class__.__name__}: {exc}")

    started = time.perf_counter()
    with ThreadPoolExecutor(max_workers=concurrency) as pool:
        list(pool.map(one, range(count)))
    return time.perf_counter() - started, latencies, errors


def drain(client: httpx.Client, api_key: str, expected: int, timeout: float) -> tuple[float, dict]:
    """Poll the stats endpoint until every notification reaches a terminal state."""
    headers = {"Authorization": f"Bearer {api_key}"}
    started = time.perf_counter()
    last: dict = {}
    while time.perf_counter() - started < timeout:
        response = client.get("/v1/stats", params={"window_seconds": 86400}, headers=headers)
        response.raise_for_status()
        last = response.json()
        by_status = last["by_status"]
        if by_status["delivered"] + by_status["dead"] >= expected:
            break
        time.sleep(0.25)
    return time.perf_counter() - started, last


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--base-url", default="http://localhost:8000")
    parser.add_argument("--api-key", required=True)
    parser.add_argument("--count", type=int, default=1000)
    parser.add_argument("--concurrency", type=int, default=16)
    parser.add_argument("--channel", default="in_app", choices=["in_app", "webhook", "email"])
    parser.add_argument("--recipient", default="http://demo-receiver:8090/hook")
    parser.add_argument("--drain-timeout", type=float, default=600.0)
    parser.add_argument("--output", default="results/loadtest.json")
    args = parser.parse_args()

    with httpx.Client(base_url=args.base_url, http2=False) as client:
        health = client.get("/healthz")
        if health.status_code != 200:
            print(f"service unhealthy: {health.text}", file=sys.stderr)
            return 1

        print(f"ingesting {args.count} notifications over {args.concurrency} threads ...")
        ingest_seconds, latencies, errors = ingest(
            client, args.api_key, args.count, args.concurrency, args.recipient, args.channel
        )
        print(f"ingest done in {ingest_seconds:.2f}s, {len(errors)} errors")
        if errors:
            for line in errors[:5]:
                print(f"  {line}", file=sys.stderr)

        print("waiting for the queue to drain ...")
        drain_seconds, stats = drain(client, args.api_key, args.count, args.drain_timeout)

    accepted = len(latencies)
    result = {
        "timestamp": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "channel": args.channel,
        "concurrency": args.concurrency,
        "requested": args.count,
        "accepted": accepted,
        "errors": len(errors),
        "ingest": {
            "seconds": round(ingest_seconds, 3),
            "requests_per_second": round(accepted / ingest_seconds, 1) if ingest_seconds else 0.0,
            "latency_ms": {
                "mean": round(statistics.fmean(latencies), 2) if latencies else 0.0,
                "p50": round(percentile(latencies, 0.50), 2),
                "p95": round(percentile(latencies, 0.95), 2),
                "p99": round(percentile(latencies, 0.99), 2),
                "max": round(max(latencies), 2) if latencies else 0.0,
            },
        },
        "delivery": {
            "seconds": round(drain_seconds, 3),
            "deliveries_per_second": round(accepted / drain_seconds, 1) if drain_seconds else 0.0,
            "by_status": stats.get("by_status", {}),
        },
    }

    output = Path(args.output)
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(result, indent=2))

    ingest_ = result["ingest"]
    delivery = result["delivery"]
    print()
    print(f"accepted            {accepted}/{args.count} ({len(errors)} errors)")
    print(f"ingest throughput   {ingest_['requests_per_second']} req/s")
    print(
        "ingest latency      "
        f"p50 {ingest_['latency_ms']['p50']}ms  "
        f"p95 {ingest_['latency_ms']['p95']}ms  "
        f"p99 {ingest_['latency_ms']['p99']}ms"
    )
    print(f"drain time          {delivery['seconds']}s ({delivery['deliveries_per_second']} deliveries/s)")
    print(f"final status        {delivery['by_status']}")
    print(f"written             {output}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

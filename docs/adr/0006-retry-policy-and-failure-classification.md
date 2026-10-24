# ADR-0006: Retry policy: jittered backoff, permanent vs retryable failures

**Status**: Accepted
**Date**: 2026-10-26

## Context

A delivery attempt can fail in ways that mean very different things:

- The receiver returned `500` — transient, worth retrying.
- The receiver returned `429` or `408` — "not now", worth retrying, ideally after
  the interval it asked for.
- The receiver returned `400` or `401` — the request is wrong or the credentials
  are wrong. Retrying the identical request will fail identically, five times,
  and each retry costs a worker slot and a third party's attention.
- The connection timed out or was refused — worth retrying.
- The request failed our own validation — should never have been queued.

Treating all of these as "retry with backoff" produces a retry storm made
entirely of requests that cannot succeed. That is the failure mode to avoid: it
is the case where retrying actively makes things worse, and it is what a naive
retry-everything policy does.

Retries also need timing. A fixed interval means every notification that failed
at the same moment is retried at the same moment, which is exactly when a
receiver that is already struggling gets a synchronised second wave.

## Decision Drivers

- **Must** never retry a failure that cannot succeed.
- **Must** not synchronise retry waves across notifications that failed together.
- **Must** keep the attempt history honest: the log has to distinguish "rejected
  once, permanently" from "tried five times, gave up".
- **Should** respect a receiver asking us to slow down.

## Considered Options

### Option 1: Retry every failure with fixed delay

- **Pros**: Trivially simple.
- **Cons**: Retries permanent failures; synchronised retry waves; burns the
  budget on requests that cannot succeed.

### Option 2: Retry every failure with exponential backoff

- **Pros**: Spreads retries; standard.
- **Cons**: Still retries permanent failures. Multiple notifications that failed
  together still back off in lockstep, so the waves are only stretched, not
  desynchronised.

### Option 3: Classify, then back off with jitter

- **Pros**: Permanent failures dead-letter immediately; transient ones back off
  exponentially with jitter, so a batch that failed together fans out over time.
- **Cons**: The classification has to be maintained — a status code whose meaning
  changes must be reconsidered here.

### Option 4: Classify, with a circuit breaker per endpoint

- **Pros**: Stops hammering a receiver that is comprehensively down.
- **Cons**: Needs per-endpoint state and a recovery policy, and interacts badly
  with many distinct webhook hosts. Not justified by the measured failure rates;
  noted as a future option rather than built.

## Decision

We will **classify failures and retry only the retryable ones**, using
**exponential backoff with equal jitter**:

```python
def backoff_seconds(attempt, base=None, cap=None):
    ideal = min(cap, base * (2 ** (attempt - 1)))
    return ideal * (0.5 + random.random() / 2)   # 50%-100% of ideal
```

Classification, in `app/channels.py`:

| Response | Outcome | Behaviour |
| --- | --- | --- |
| `2xx` | `delivered` | terminal |
| `408`, `429`, any `5xx` | `failed` | retry with backoff |
| other `4xx` (e.g. `400`, `401`, `404`) | `permanent` | dead-letter immediately |
| connection error, timeout | `failed` | retry with backoff |

## Rationale

The decisive driver was not retrying work that cannot succeed. A `401` from a
customer's endpoint will still be a `401` in thirty seconds; sending it five
times costs a worker slot, pollutes the attempt log, and in the worst case looks
like an attack from the receiver's side.

Equal jitter (50–100% of the ideal delay) was chosen over full jitter (0–100%)
because full jitter allows a later attempt to be scheduled *before* an earlier
one for the same notification, which makes the backoff ordering unreadable in
timings. The narrower band still desynchronises a wave — it is enough that two
notifications failing together do not retry together.

Dead-lettering a permanent failure immediately also keeps the retry budget
meaningful: `max_attempts` is spent on genuine uncertainty, not on repetitions of
a known-bad request.

## Consequences

### Positive

- Permanent failures terminate in one attempt instead of five.
- Verified: a receiver returning `400` produced `status=dead, attempts=1` with
  `last_error = "rejected permanently: receiver rejected the delivery with 400"`.
- Retry waves fan out instead of synchronising.
- Verified ladder against a receiver returning `503` twice:
  attempt 1 at t+0, attempt 2 at t+1.035 s, attempt 3 at t+1.565 s, then `200`
  and `delivered` — matching an ideal of 1 s then 2 s at 50–100% jitter.
- The distinction an operator needs is preserved: `last_error` records *why
  retrying stopped*, while the transport's own message stays in the attempt log,
  so "dead after one rejection" and "dead after five tries" are distinguishable.

### Negative

- The status-code classification is a maintenance burden: `429` being retryable
  is a decision, not a universal truth, and `408`/`429` are singled out in code
  as `RETRYABLE_STATUS`.
- A receiver that returns `400` for a transient reason is now never retried —
  the customer has to use the `retry` endpoint manually. Accepted: `400`
  semantically means "do not retry this".
- No respect for `Retry-After` on a `429` yet; the backoff schedule is used
  instead.

### Risks

- **Misclassified transient failure permanently dead-letters work.** Mitigated by
  the manual retry endpoint (`202` for `dead`, `409` otherwise), which is the
  designed escape hatch.
- **Cap too low relative to receiver recovery time.** `retry_cap_seconds` must
  exceed a plausible outage. The compose default is deliberately small (1 s base,
  30 s cap) so the ladder is visible in a demo.

## Implementation Notes

- `app/tasks.py`: `backoff_seconds`, `attempt_delivery`.
- `app/channels.py`: `RETRYABLE_STATUS = {408, 429}` and the `5xx` check;
  `PermanentFailure` is the exception that carries a status code out of a
  channel sender.
- Retries are **scheduled on the broker** (`countdown`), never `sleep`-ed inside
  a thread. This is a hard requirement: sleeping would pin a worker slot.
- `retry_base_seconds`, `retry_cap_seconds`, `max_attempts_default` are settings.
- The manual retry endpoint **tops up** the budget rather than resetting it, so
  attempt numbers stay monotonic in the log.

## Related Decisions

- ADR-0005: how a failed attempt interacts with redelivery and the state machine.
- ADR-0009: the same "classify the failure, do not retry blind" reasoning applied
  to the analytics sink.

## References

- `app/tasks.py`, `app/channels.py`, `tests/test_delivery.py`
- README, *Delivery lifecycle*.

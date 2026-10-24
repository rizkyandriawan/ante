# ADR 0006: atomic result completion

Date: 2026-10-24
Status: accepted

## Context

Workers normally publish a result and then acknowledge their input. A crash
between those commits redelivers the input, duplicating the result. Doing the
ack first is worse: a crash can lose the result while removing the work.

## Decision

- Add `complete(token, queue, message, settlement)` to the engine.
- In one immediate SQLite transaction: verify the token, verify the destination
  queue, insert the result, finish the input attempt, and delete the input.
- A stale or unknown token publishes nothing. Any validation or storage error
  aborts the transaction and leaves the input in flight.
- `settlement` is `Ack` for success or `Discard` for a terminal failure.
- Expose `COMPLETE` in the native protocol and `Delivery.complete` in the
  embedded Python API. AgentQueue uses it for both `.results` and `.failed`.

## Consequences

The duplicate-result window inside the queue database is closed. External side
effects remain outside the transaction. The result and input must share one
engine/database; cross-database completion is not atomic. Reply metadata is
carried by the embedded API; the native command supports content type and
application message id only.

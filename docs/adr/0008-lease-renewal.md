# ADR 0008: renewable delivery leases

Date: 2026-10-24
Status: accepted

## Context

A fixed lease forces a trade-off: make it long enough for slow work and dead
workers hold messages for too long; make it short and legitimate long handlers
are redelivered. Requeue-and-republish is not an option for work with external
side effects.

## Decision

- Add `touch(token, lease_ms)` to the engine. It updates the in-flight entry's
  deadline only when the delivery id still matches.
- `lease_ms = 0` uses the engine default.
- Stale and unknown tokens do not extend anything.
- Expose `TOUCH token [lease-ms]` in the native protocol and
  `Delivery.touch()` in the embedded Python API.
- Keep attempt history unchanged; renewal is not a new attempt.
- Do not add an AMQP extension method. AMQP consumers pick a lease that covers
  their handler or split work into shorter units.

## Consequences

Long-running native/embedded work can renew without changing the at-least-once
contract. A worker that stops renewing still loses the lease and is retried.
Renewal is cooperative: it cannot prove the handler is making progress.

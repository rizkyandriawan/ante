# ADR 0007: bounded delivery-attempt history

Date: 2026-10-24
Status: accepted

## Context

Attempt history is useful for status, but keeping every finished row forever
turns an operational aid into an unbounded database. Unfinished attempts must
survive regardless: they describe work that may still be settled.

## Decision

- Keep finished history for 7 days and at most 1,000,000 rows by default.
- Expose both limits in `Options`; zero disables that limit.
- Never prune an attempt whose `finished_at_ms` is null.
- Prune at engine open and every 60 seconds while a native or AMQP listener is
  running.
- Delete at most 10,000 rows per run, oldest first, with an index on
  `(finished_at_ms, delivery_id)`.
- Row-count pruning keeps the newest finished attempts and removes older rows.
- Expose explicit pruning and retention settings in the embedded Python API.

## Consequences

History is bounded without a separate maintenance daemon. A large backlog may
take several ticks to drain, but each tick has a bounded write. After pruning,
`STATUS` for an old finished message returns `MESSAGE_NOT_FOUND`; callers that
need longer audit retention must export history before the limit.

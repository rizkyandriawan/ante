# ADR 0005: durable delivery attempt history

Date: 2026-10-24
Status: accepted

## Context

Queue counters show how much work is pending, but not what happened to one
message. Operators need to answer whether a job is ready, in flight, retried,
acked, expired, or discarded, and when each attempt started and ended. AgentQueue
currently keeps this in human-readable JSON files, outside the queue database.

## Decision

- Add `delivery_attempts`, keyed by delivery id, with message and queue identity
  snapshots, attempt number, start/finish timestamps, and a typed outcome.
- Insert an `inflight` row in the same transaction as each take.
- Finish the row in the same transaction as ack, requeue, expiry, startup
  recovery, or queue deletion.
- Do not use a foreign key to entries; history must survive ack and queue
  deletion.
- Add `STATUS queue message-id` to the native protocol and `message_status` to
  the embedded Python API.
- Bump the SQLite schema to version 2 and migrate version-1 databases in one
  transaction, backfilling timestamps and in-flight attempt rows.

## Consequences

Status queries are indexed and do not scan ready work. History grows with
attempts, so retention must be designed before it is exposed as a long-term job
log. The queue entry remains the source of truth; history is diagnostic. AMQP
has no portable status method, so this surface is native/embedded only for now.

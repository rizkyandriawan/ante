# ADR 0003: SQLite replaces redb for the embedded queue

Date: 2026-10-24
Status: accepted

## Context

Ante was initially optimized as a standalone, high-throughput queue. The
product direction changed: Ante is primarily the embedded delivery layer under
AgentQueue. The important workload is a small number of long-running jobs,
status polling, attempts, results, and durable history.

ADR 0001 selected redb for durable commit latency and bounded memory. Those
properties remain useful, but the new workload puts more weight on relational
queries, indexes, operational diagnostics, and transactional coordination
between queue state and job lifecycle records.

## Decision

Use SQLite through `rusqlite` with bundled SQLite as the only active storage
backend. Run with WAL and `synchronous=FULL`. Keep one engine owner per database
file so startup recovery cannot run from a polling connection.

There is no redb data migration: the cutover starts from a fresh database.

## Consequences

Positive:

- Job status, attempts, history, and pagination can use indexed SQL.
- One transactional boundary can eventually cover result storage and queue
  settlement.
- Standard SQLite tooling and backup APIs are available.
- The queue remains embeddable and serverless.

Negative:

- Native SQLite is compiled into every build.
- WAL has a single writer; write concurrency must stay coordinated.
- Long readers can delay checkpointing.
- SQLite and WAL files make backup more involved than copying one file.
- Historical redb performance numbers no longer describe the active backend.

## Guardrails

- No network call, Python callback, or agent execution inside a transaction.
- No second engine owner and no recovery from a reader connection.
- No automatic retry of non-idempotent operations with ambiguous commit
  outcomes.
- No silent replacement of an unknown or foreign database file.
- SQLite constraints are defense in depth, not a substitute for domain checks.

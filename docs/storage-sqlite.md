# SQLite storage

Ante stores queues in a single SQLite database. The default path is
`./ante.sqlite3`; `ANTE_DB` or `--db` overrides it.

## Runtime configuration

- WAL journal mode.
- `synchronous=FULL` on the writer.
- `foreign_keys=ON`.
- A bounded writer and two bounded query-only reader connections.
- An exclusive `.lock` file beside the database.

The lock is an **engine ownership** lock, not just a SQLite lock. Opening the
same database twice fails with `ENGINE_OPEN`; it never runs startup recovery
from a polling connection. A completed clean shutdown releases the lock.

## Startup behavior

1. Acquire the owner lock.
2. Reject a non-SQLite or foreign SQLite file without overwriting it.
3. Validate the schema version; a version-1 database is migrated to version 2
   in one write transaction.
4. Open the writer and apply SQLite settings.
5. Requeue in-flight deliveries from the previous process.
6. Open query-only reader connections and start listeners.

## Backup and restore

Stop the engine before an offline backup. Prefer the SQLite backup API when
writing a backup tool; copying only the main database file while WAL writes are
active is not a consistent snapshot.

If the whole Ante/AgentQueue home is archived after a clean shutdown, include
the SQLite database and any remaining `-wal`/`-shm` files together. Restore the
files as a set, then open the database once to let SQLite recover WAL state.

## Attempt history

Schema version 2 adds `created_at_ms`/`updated_at_ms` to entries and a
`delivery_attempts` table keyed by delivery id. Each take inserts an `inflight`
attempt; ack, requeue, lease expiry, startup recovery, and queue deletion finish
it with `acked`, `requeued`, `expired`, `recovered`, or `discarded`. A message
removed while ready has no new attempt; its status reports `discarded` while
history retains the previous outcome.

The table deliberately has no foreign key to entries. Acknowledging a message or
deleting its queue therefore preserves attempt history for status queries.

Retention defaults to **7 days** and **1,000,000 finished attempts**. The policy
runs at open and every 60 seconds while a native or AMQP listener is running.
Unfinished attempts are never pruned. Set `history_retention_ms=0` or
`history_max_rows=0` to disable that limit. Each run deletes at most 10,000 rows
so a large backlog cannot stall delivery indefinitely.

## Limits and operational notes

- SQLite has one writer. Long write transactions block other writers.
- Long-lived read transactions can delay WAL checkpointing. Polling queries are
  indexed and short by design.
- The database is not a multi-process API. Only the Ante engine owner writes it.
- The queue database now contains delivery attempt history. AgentQueue's
  human-readable job records remain a separate store until the job-history API
  is adopted there.
- Process-crash tests do not prove power-loss durability on every device.

## Verification

- `qa/reports/2026-10-24-sqlite-probe.md`: three 10,000-job runs, zero loss,
  zero in-flight remainder, p95 polling latency below 8 ms.
- `qa/reports/2026-10-24-fault-matrix.md`: 15 `SIGKILL` cycles covering committed
  publish, in-flight recovery, committed ack, atomic completion, and owner lock;
  15/15 passed.
- These are local process-crash checks. They do not prove power-loss durability
  on every filesystem or device.

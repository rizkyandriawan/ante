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
3. Validate the schema version.
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

## Limits and operational notes

- SQLite has one writer. Long write transactions block other writers.
- Long-lived read transactions can delay WAL checkpointing. Polling queries are
  indexed and short by design.
- The database is not a multi-process API. Only the Ante engine owner writes it.
- The queue database and AgentQueue job history may be adjacent SQLite files or
  tables in a future version; they are not merged by this cutover.
- Process-crash tests do not prove power-loss durability on every device.

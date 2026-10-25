# Storage comparison: SQLite vs redb — 2026-10-24

A fair like-for-like comparison of Ante's SQLite engine and the historical redb
engine, followed by two low-risk SQLite hot-path changes.

## Workload

Both engines run the same loop with the same public API shape:

```text
publish_batch(n) -> take(n) -> ack_batch(n)
```

- 1 KiB payload per job.
- One process, one queue, one writer.
- SQLite: WAL, `synchronous=FULL`, `foreign_keys=ON`, owner lock, two idle
  query-only readers.
- redb 2.6: default `Durability::Immediate` (durable commit).
- Three repetitions per invocation; each row below is the median of one
  invocation. The batch-128 table also shows three independent invocations to
  expose host noise.
- No AMQP or native-socket layer is involved. These numbers measure storage and
  engine work, not end-to-end broker throughput.

Harness: `/tmp/storage-compare` (`publish_batch` → `take` → `ack_batch`).
The redb engine is the pre-SQLite `ante-core` extracted to
`/tmp/ante-redb-core`, renamed and updated to redb 2.6.

## Results

### Batch 1, 20,000 jobs

| Engine | Jobs/s | publish p50 | take p50 | ack p50 |
|---|---:|---:|---:|---:|
| SQLite before optimization | 981.5 | 199 µs | 239 µs | 207 µs |
| SQLite after optimization | 1,212.7 | 185 µs | 203 µs | 180 µs |
| redb | 1,740.1 | 168 µs | 168 µs | 156 µs |

SQLite improved ~24%. redb remains ~1.44× faster at one job per transaction.

### Batch 128, 100,000 jobs

| Engine | Jobs/s (3 invocations) | Median | publish p50 | take p50 | ack p50 |
|---|---:|---:|---:|---:|---:|
| SQLite before optimization | 26,332 (single invocation) | 26,332 | 705 µs | 1,578 µs | 1,761 µs |
| SQLite after optimization | 43,644 / 43,463 / 32,969 | 43,463 | ~1,050 µs | ~1,055 µs | ~1,005 µs |
| redb | 65,138 / 64,104 / 64,128 | 64,128 | ~648 µs | ~515 µs | ~652 µs |

SQLite improved ~65% on the median. redb remains ~1.48× faster. The third
SQLite invocation was a host-noise outlier; its p99 values were an order of
magnitude worse while p50 stayed flat.

### Batch 256, 100,000 jobs

| Engine | Jobs/s | publish p50 | take p50 | ack p50 |
|---|---:|---:|---:|---:|
| SQLite after optimization | 52,300 | 999 µs | 1,725 µs | 1,733 µs |
| redb | 66,408 | 1,233 µs | 1,025 µs | 1,306 µs |

Batch 256 is ~20% faster than batch 128 on SQLite and leaves redb ~1.27× ahead.
One earlier batch-256 pair was invalidated by a host I/O stall (SQLite fell to
~5.9k and redb to ~6.9k jobs/s simultaneously); repeat runs were stable at the
numbers above. The SQLite write transaction is capped at 256 messages by the
default `max_batch_messages`, so larger batches require an options change.

### Changes kept

1. **Prepared-statement cache:** writer and readers now cache 64 statements.
   The hot paths (`take`, `settle_batch`, `insert_messages_tx`, metadata and
   sequence updates, requeue) use `prepare_cached` instead of parsing a new
   statement per call. Batch-128 improved from ~26.3k to ~29.2k jobs/s before
   the index change.
2. **Attempt-history indexes:** the unused `attempts_queue(queue_id,
   started_at_ms)` index was removed, and `attempts_finished` is now partial:
   `WHERE finished_at_ms IS NOT NULL`. Inserts no longer maintain a finished
   index entry for every in-flight attempt, and settlements update a smaller
   index. Batch-128 moved from ~29.2k to ~43.5k jobs/s median.

The index change is applied on open and does **not** bump `SCHEMA_VERSION`
(remains 2): it is non-semantic, and a schema-2 engine can still open the
optimized database. A regression test rebuilds a legacy non-partial index and
removes the queue index.

## Diagnostic variants (not merged)

Throwaway engine copies isolated the remaining gap:

| Variant (batch 128) | Jobs/s |
|---|---:|
| Current optimized engine | ~43.5k–44.6k |
| History writes removed | ~59.7k |
| `synchronous=NORMAL` | ~62.6k |
| History removed + `synchronous=NORMAL` | ~113.7k |
| Bulk claim-insert + bulk settle SQL (history kept, FULL) | ~49.6k |

Conclusions:

- Attempt-history writes and their indexes account for the largest remaining
  SQLite-specific cost.
- `synchronous=FULL` is a material share at batch 1 and still visible at batch
  128. We do not weaken the durable-commit default.
- Bulk SQL is measurable but modest (~11% over the optimized per-row code) and
  adds dynamic-SQL complexity, so it was not kept.
- Disabling or deferring attempt history would trade an existing feature
  (status/audit history) for throughput. That is a product decision, not a
  storage micro-optimization, and was not made here.

### Cycle-based fsync (not implemented)

SQLite has no "fsync every N milliseconds" durability mode. `synchronous=NORMAL`
only guarantees database consistency, not durability of the latest commits, so
it is not a substitute for a durable queue. A safe cycle design is **group
commit** inside Ante's writer:

1. Serialize write intents through one writer queue instead of locking SQLite
   from each API call.
2. Drain pending intents for at most a short cycle (or until a byte/count cap).
3. Apply them in one transaction, commit once, fsync once.
4. Reply to each caller only after that commit succeeds.

This amortizes fsync without weakening the post-commit reply contract, but it is
an engine architecture change, not a pragma. It only helps when writes are
concurrent: a single sequential client would just pay the cycle latency. It also
needs per-request error isolation (one invalid request must not roll back the
group), bounded memory/backpressure, and tests for read-after-write within a
pending group. A cheaper adaptive variant is to coalesce only requests that
arrive while a commit is already in flight, adding no delay when the writer is
idle.

No cycle implementation or benchmark was run in this report.

## Verification

- `cargo test -p ante-core`: all tests pass, including the legacy-index rebuild
  test and the version-1 migration test.
- Full workspace: **132 passed, 0 failed**.
- Clippy (six Ante packages, all targets, `--no-deps -- -D warnings`): passed,
  0 warnings. `cargo fmt --all -- --check`: passed.
- Downstream debug + release build without `[patch.crates-io]`: passed.

# Ante storage benchmark

Compares redb and LMDB (via `heed`) on a work-queue workload: durable
enqueue, claim, ack, delayed retry, backlog, and crash recovery.

## Build

```bash
export PATH=$HOME/.cargo/bin:$PATH
cargo build --release
```

Binaries: `target/release/redb-bench`, `target/release/heed-bench`.

## Run

Full suite (lifecycle matrix, backlog, retry, 1M footprint, kill -9 recovery):

```bash
./run.sh
```

Results are written to `results/<timestamp>.jsonl` and `.meta`.

Single run:

```bash
./target/release/redb-bench stream --db=/tmp/ante-bench/x \
  --total=20000 --batch=64 --payload=1024
./target/release/heed-bench recover --db=/tmp/ante-bench/x \
  --total=1 --batch=1 --payload=1024
```

Modes: `stream`, `backlog`, `retry`, `fill`, `idle`, `recover`.
Flags: `--db`, `--total`, `--batch`, `--payload`, `--retry-pct`, `--seed`, `--rep`.

## Analyze

```bash
python3 analyze.py
```

## End-to-end protocol benchmark

Measures the real `ante-core` engine plus the native and AMQP servers over
loopback TCP (server and client in one process). It publishes and consumes
unique payloads, verifies every job exactly once, and reports throughput, batch
latencies, peak RSS, and database size.

```bash
./e2e.sh              # ~3 minutes; writes results/<id>-e2e.jsonl
./analyze_e2e.py      # markdown table of medians
```

Single run:

```bash
./target/release/e2e-bench native --total=51200 --batch=256 --payload=1024
./target/release/e2e-bench amqp --total=12800 --batch=128 --payload=1024 --mode=publish
./target/release/e2e-bench amqp --total=12800 --batch=256 --payload=1024 --mode=consume
./target/release/e2e-bench core --total=51200 --batch=128 --payload=1024
```

Flags: `--total`, `--batch` (1..256), `--payload`, `--rep`, `--window`
(batches of backlog allowed), `--mode=both|publish|consume` (AMQP only).

AMQP publish is one durable commit per `basic.publish`; `--mode=publish` runs
without a consumer and verifies the stored queue after the timed phase.
`consume` pre-fills before timing; `both` starts the producer and consumer
together, and consume paths wait for a server-side ack-commit barrier. See
`RESULTS.md` section 8.

## Notes

- Both stores use default durable commits. Do not compare these numbers with
  benchmarks that disable fsync.
- LMDB memory is file-backed mmap. `rss_kb` includes resident mapped pages;
  `anon_kb` and `file_kb` split them. Idle after transaction shows the small
  steady-state footprint.
- `kill -9` tests validate logical recovery, not power-loss durability.
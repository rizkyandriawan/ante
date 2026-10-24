# SQLite workload probe — 2026-10-24

Three-run workload probe on the embedded Python SDK, rerun after the AMQP 1.0
cancellation changes. It is a local development measurement, not a release
capacity claim.

Workload: 10,000 published 1 KiB jobs in batches of 100, 16 worker threads using
`take(max=32, wait_ms=20)`, and one thread polling `info("jobs")` every 20 ms.
Each run used a fresh temporary database with WAL + `synchronous=FULL`.

Integrity checks use indexed payloads, not only delivery tokens. Every ACK
result count and status is checked, workers and the poller propagate exceptions,
all joins share a 120-second global deadline, and a 125-second supervisor kills
the child if it does not finish. Each run asserts exactly 10,000 unique messages
with zero duplicates and an empty queue.

Environment: Linux x86_64, 20 logical CPUs, ext4 on NVMe, Python 3.12.3,
Ante Python tree `ce6034a` plus the local SQLite/AMQP work.

| Metric | Run 1 | Run 2 | Run 3 | Median |
|---|---:|---:|---:|---:|
| Publish phase | 0.170 s | 0.171 s | 0.166 s | 0.170 s |
| Drain phase | 0.847 s | 0.843 s | 0.830 s | 0.843 s |
| Acknowledged | 10,000 | 10,000 | 10,000 | 10,000 |
| Unique messages | 10,000 | 10,000 | 10,000 | 10,000 |
| Duplicates | 0 | 0 | 0 | 0 |
| Remaining ready/in-flight | 0 | 0 | 0 | 0 |
| Poll p50 | 2.80 ms | 3.15 ms | 3.27 ms | 3.15 ms |
| Poll p95 | 5.78 ms | 6.05 ms | 5.24 ms | 5.78 ms |
| Poll max | 9.57 ms | 11.57 ms | 11.88 ms | — |
| Database size | 13.36 MiB | 13.37 MiB | 13.39 MiB | 13.37 MiB |
| WAL after drain | 4.04 MiB | 4.08 MiB | 4.03 MiB | 4.04 MiB |

Limits: the poller produced 37–51 samples per run, so tail latency is indicative
rather than statistically strong. The probe does not cover retention-heavy
history, long-running agent execution, multi-process access, or power-loss
durability. WAL size is measured before closing the engine, so it includes
uncheckpointed pages.

Reproduce with `python qa/sqlite_probe.py` using the rebuilt Python extension.

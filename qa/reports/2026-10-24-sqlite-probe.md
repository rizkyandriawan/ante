# SQLite probe — 2026-10-24

Preliminary workload probe, not a release benchmark. One run on the local
development machine; three repetitions remain required before accepting a
capacity claim.

Workload: 10,000 published 1 KiB jobs, 16 worker threads, 20 ms polling cadence,
SQLite WAL + `synchronous=FULL`, embedded Python SDK.

| Metric | Result |
|---|---:|
| Publish batch phase | 0.98 s |
| Drain phase | 1.27 s |
| Acknowledged | 10,000 / 10,000 |
| Remaining ready/in-flight | 0 |
| Poll p50 | 5.1 ms |
| Poll p95 | 11.4 ms |
| Poll max | 17.4 ms |
| Database size | 14.0 MB |
| WAL after drain | 4.37 MB |

The probe met the plan's initial `p50/p95` polling target (p95 <= 50 ms) and
completed with zero loss. It does not cover retention-heavy history, three
repetitions, crash injection, or long-running agent execution.

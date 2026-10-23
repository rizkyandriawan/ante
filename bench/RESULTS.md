# Ante storage benchmark: redb vs LMDB (`heed`)

Date: 2026-10-23. Raw results: `results/*.jsonl`. Reproduce with `./run.sh`
(see `README.md`). Numbers below are measured on this machine, not projections.

## Verdict

**redb is the better default for Ante.** It has ~1.8x lower durable commit
latency when publishing one job at a time, keeps anonymous memory bounded
(~65 MB at a 1M-job backlog), and is pure Rust with no native dependency.

**LMDB is the better specialist** if the priority is fast full scans, the
smallest idle footprint, or the smallest file for tiny payloads. Its mmap
footprint is reclaimable page cache, not a leak, but it is charged to the
process/cgroup and looks large in `ps`.

The largest product lesson is not "which store wins": **group commit is
mandatory.** Per-message durable commits cap both stores at ~1–2k jobs/s.
Batching 128 messages raises this to ~110–124k jobs/s for small payloads.

## Environment

| Item | Value |
|---|---|
| CPU | 13th Gen Intel Core i7-13700H, 20 threads |
| RAM | 15.7 GiB |
| Kernel | 7.0.0-38-generic |
| Filesystem | ext4 (`stat -f` reports ext2/ext3 family), NVMe |
| Rust | 1.99.1, release, LTO, stripped |
| redb | 2.6.4 |
| heed | 0.22.1 (LMDB 0.9.x via `lmdb-master-sys` 0.2.6, bundled) |
| Benchmark binary | redb 1,223,992 B; heed 553,136 B |

## Method

- One queue, one process, sequential phases. Payload is opaque bytes stored in
  the same database as queue state.
- Keyspace: `jobs` (u64 → payload), `ready` (seq → job id), `inflight`
  (seq → job id), `delayed` ((due_ms, seq) → job id), `meta` (next seq).
- Each lifecycle cycle performs three durable commits: enqueue batch, claim
  batch, ack batch. `claim` moves ready → inflight; `ack` removes both.
- Both stores use their **default durable commit** (redb `Durability::Immediate`;
  LMDB synchronous commit). No `NOSYNC`.
- redb cache is capped at 64 MiB. LMDB map size is 8 GiB (virtual reservation;
  only allocated pages are counted).
- Lifecycle and bulk-enqueue figures are medians of 3 runs. Backlog and retry
  are single runs and are noisier.
- Process-crash tests use `kill -9`; they do not prove power-loss durability.

Limitations: single producer/consumer (no concurrent clients), no AMQP layer,
page cache not dropped between runs, one machine.

## 1. Lifecycle: durable enqueue → claim → ack

Medians of 3 runs. "Publish-confirm /msg" is the enqueue commit latency
divided by batch size.

| Payload | Batch | Backend | Durable jobs/s | Confirm p50/msg | p99/msg | Claim p50/msg | Peak RSS |
|---:|---:|---|---:|---:|---:|---:|---:|
| 64 B | 1 | redb | 1,994 | 161 us | 261 us | 155 us | 5 MB |
| 64 B | 1 | lmdb | 1,090 | 290 us | 422 us | 288 us | 3 MB |
| 64 B | 32 | redb | 54,054 | 5.6 us | 8.4 us | 5.9 us | 5 MB |
| 64 B | 32 | lmdb | 33,613 | 9.3 us | 15.3 us | 9.3 us | 2 MB |
| 64 B | 128 | redb | 113,455 | 2.2 us | 4.8 us | 2.9 us | 5 MB |
| 64 B | 128 | lmdb | 124,025 | 2.3 us | 3.6 us | 2.5 us | 2 MB |
| 1 KiB | 1 | redb | 1,997 | 160 us | 245 us | 155 us | 5 MB |
| 1 KiB | 1 | lmdb | 1,092 | 289 us | 428 us | 287 us | 3 MB |
| 1 KiB | 32 | redb | 46,620 | 7.2 us | 15.8 us | 6.0 us | 5 MB |
| 1 KiB | 32 | lmdb | 33,333 | 10.0 us | 17.8 us | 9.1 us | 3 MB |
| 1 KiB | 128 | redb | 88,354 | 4.2 us | 7.7 us | 3.0 us | 5 MB |
| 1 KiB | 128 | lmdb | 110,320 | 3.2 us | 8.4 us | 2.5 us | 3 MB |
| 8 KiB | 1 | redb | 1,952 | 172 us | 259 us | 155 us | 5 MB |
| 8 KiB | 1 | lmdb | 918 | 298 us | 894 us | 292 us | 3 MB |
| 8 KiB | 32 | redb | 13,423 | 47.7 us | 236 us | 7.1 us | 6 MB |
| 8 KiB | 32 | lmdb | 13,831 | 46.7 us | 106 us | 9.3 us | 4 MB |
| 8 KiB | 128 | redb | 22,436 | 33.3 us | 69 us | 3.4 us | 9 MB |
| 8 KiB | 128 | lmdb | 32,363 | 22.1 us | 76 us | 2.5 us | 7 MB |

Reading:

- At batch 1 redb's durable commit is ~160 us vs LMDB's ~290 us: redb is ~1.8x
  faster where every publish must be confirmed.
- At batch 128 the roles reverse for 1 KiB and 8 KiB payloads: LMDB is 1.25x
  and 1.44x faster. For 64 B they are within 10%.
- batching is the dominant factor: 32–107x throughput improvement over
  per-message commits, at unchanged durability.

## 2. Backlog: enqueue then drain

| Payload | Jobs | Backend | Enqueue/s | Drain/s | RSS after enqueue | Peak RSS | Disk |
|---:|---:|---|---:|---:|---:|---:|---:|
| 64 B | 500,000 | redb | 74,416 | 74,660 | 39 MB | 39 MB | 94 MB |
| 64 B | 500,000 | lmdb | 97,847 | 85,985 | 56 MB | 56 MB | 56 MB |
| 1 KiB | 200,000 | redb | 66,778 | 78,125 | 65 MB | 65 MB | 423 MB |
| 1 KiB | 200,000 | lmdb | 56,211 | 80,000 | 197 MB | 400 MB | 417 MB |
| 8 KiB | 50,000 | redb | 14,715 | 50,302 | 63 MB | 64 MB | 825 MB |
| 8 KiB | 50,000 | lmdb | 17,313 | 56,689 | 17 MB | 592 MB | 617 MB |

LMDB's RSS after enqueue is unstable because it is file-backed mmap: pages are
resident while dirty and can be reclaimed afterwards (the 8 KiB row sampled at
17 MB but peaked at 592 MB). redb shows a stable ~65 MB (its 64 MiB cache).

## 3. Bulk enqueue, 1 KiB, and idle footprint

Medians of 3 runs. Disk is allocated blocks, not apparent file size.

| Jobs | Backend | Enqueue/s | Disk | Peak RSS (fill) | Idle RSS after open |
|---:|---|---:|---:|---:|---:|
| 200,000 | redb | 73,855 | 423 MB | 65 MB | 4 MB |
| 200,000 | lmdb | 55,096 | 417 MB | 197 MB | 2 MB |
| 1,000,000 | redb | 41,281 | 2,113 MB | 66 MB | 4 MB |
| 1,000,000 | lmdb | 40,940 | 2,084 MB | 972 MB | 2 MB |

The 1M-job runs varied a lot across repetitions for **both** stores
(redb 37.9k–72.4k/s, LMDB 40.5k–53.4k/s), consistent with writeback pressure
from repeatedly writing 2.1 GB. Treat the 1M row as "I/O bound, roughly equal"
and the 200k row as the cleaner efficiency comparison.

Disk overhead at 1M jobs is ~2.1x the 1 GiB payload for both. For very small
payloads redb is heavier: 500k x 64 B uses 94 MB vs LMDB's 56 MB.

## 4. Recovery and scan

| DB | Backend | Open | Full integrity scan | RSS after scan |
|---|---|---:|---:|---:|
| 1,000,000 jobs | redb | 1.8 ms | 1,991 ms | 65 MB |
| 1,000,000 jobs | lmdb | ~0.05 ms | 315 ms | 1,990 MB |

The scan is this benchmark walking every entry, not something Ante needs at
startup. LMDB scans faster; redb stays memory-bounded while scanning.

## 5. Retry-heavy workload

20% of claimed jobs moved to a due-time index and promoted back, batch 64.

| Backend | State transitions/s | Peak RSS |
|---|---:|---:|
| redb | 76,046 | 66 MB |
| lmdb | 61,200 | 138 MB |

## 6. Process-kill recovery

30 `kill -9` cycles per backend, each followed by a full invariant check
(every ready/inflight/delayed reference must point at an existing job and be
live exactly once).

| Backend | Cycles | Invariant failures | Duplicate live refs | Missing-job refs |
|---|---:|---:|---:|---:|
| redb | 30 | 0 | 0 | 0 |
| lmdb | 30 | 0 | 0 | 0 |

## Decision for Ante

Choose **redb** as the v1 backend:

1. Lower latency for the common case where a publish must be durably confirmed.
2. Anonymous memory is bounded independently of backlog size, which matters for
   a service that claims a small footprint and runs next to applications.
3. Pure Rust: one toolchain, simpler cross-platform builds, no `map_size`
   management or native library packaging.
4. Correct under the crash test.

Keep the storage boundary narrow so LMDB can be added later if a workload
proves to need fast full scans or the smallest possible file for tiny payloads.
Do not treat RSS alone as the memory metric for LMDB: separate anonymous from
file-backed memory, and note that mapped pages are reclaimable but still
charged to a cgroup.

Regardless of backend, implement **group commit** (or batch publish/ack) in
Ante's engine: all the high numbers in this report depend on it.
## 7. Sustained run: redb, 1 KiB, batch 256, 15 seconds

Three consecutive 15 s runs, same workload as the lifecycle test (enqueue 256,
claim 256, ack 256, all durable commits). Raw data:
`results/20261023T074504Z-sustained.jsonl`.

| Rep | Jobs completed | Jobs/s | Confirm p50/msg | Confirm p99/msg | Peak RSS | DB file |
|---:|---:|---:|---:|---:|---:|---:|
| 1 | 1,523,456 | 101,550 | 3.96 us | 6.38 us | 6.3 MB | 1.9 MB |
| 2 | 1,478,656 | 98,577 | 4.06 us | 6.72 us | 6.0 MB | 1.9 MB |
| 3 | 1,414,400 | 94,281 | 4.11 us | 9.59 us | 6.2 MB | 1.9 MB |

Per-second throughput across all 45 one-second windows: min 71,936,
p10 95,232, median 98,816, max 104,192 jobs/s. 43 of 45 windows were at or
above 94k jobs/s; the two low windows were the final two seconds of rep 3.

Reading:

- ~98.6k jobs/s sustained for 15 s, not just a burst. Throughput held within
  roughly 6% for the first 28 seconds of measurement.
- The run committed 16.6k–17.9k times per 15 s (~1.1k–1.2k fsync/s). At batch
  256, each fsync covers 256 messages; per-commit latency is ~840 us.
- The database stayed at 1.9 MB while ~1.5M jobs (1.5 GB of payload) passed
  through per run. Reclamation/reuse works in steady state.
- Peak RSS stayed ~6 MB.
- Rep 3's tail (84k, 72k jobs/s) is a warning that longer runs may hit
  writeback/thermal limits; 15 s is not a multi-hour soak test.

Conditions: single process, one queue, sequential producer/consumer, 1 KiB
payload, batch 256, default durable commits, local NVMe/ext4, no network or
AMQP parsing, no handler work. The figure counts completed queue jobs, each
costing three durable commits; it is not 100k independent fsyncs per second.

## 8. End-to-end over TCP: native protocol and AMQP

Date: 2026-10-23. Raw data: `results/20261023T172245Z-e2e.jsonl` (72 runs).
Reproduce with `./e2e.sh`; summarize with `./analyze_e2e.py`.

This phase measures the real server stack: redb, `ante-core`, and the wire
protocols over loopback TCP. Server and client run in the same process (no
separate daemon process, no TLS), so it is a wire-path measurement, not a
network measurement.

Method: one queue, one producer connection, one consumer connection, durable
commits, 64 MiB redb cache, 3 reps (median reported). A bounded window of 8
batches keeps the backlog small. Native uses `PUBLISH_BATCH` / `TAKE` /
multi-token `ACK`. AMQP uses publisher confirms and `basic.ack multiple=true`
with prefetch equal to the batch. Totals: native 51,200 jobs (2,000 at batch
1), AMQP 12,800 jobs (512 at batch 1).

Each mode measures one path: `publish` has no consumer and verifies the stored
queue after the timed phase; `consume` pre-fills through the engine before the
timer starts and measures only delivery + acknowledgement; `both` runs producer
and consumer together with **no pre-fill**. In consume/both, the timer stops
only after a passive `queue.declare` barrier on the same channel, which proves
the server committed the final `basic.ack` (`basic.ack` itself has no reply).
`verified` is true only after every payload was checked, and every run asserts
that the queue is empty at the end.

| Protocol | Mode | Payload | Batch | Jobs/s | Publish batch p50 | Consume batch p50 | Peak RSS |
|---|---|---:|---:|---:|---:|---:|---:|
| native | both | 64 B | 1 | 1,381 | 255 us | 703 us | 8 MB |
| native | both | 64 B | 128 | 54,877 | 1.31 ms | 2.07 ms | 10 MB |
| native | both | 64 B | 256 | 76,190 | 2.00 ms | 3.19 ms | 10 MB |
| native | both | 1 KiB | 1 | 1,311 | 247 us | 677 us | 9 MB |
| native | both | 1 KiB | 128 | 41,424 | 1.86 ms | 2.79 ms | 15 MB |
| native | both | 1 KiB | 256 | 31,411 | 4.17 ms | 5.77 ms | 19 MB |
| amqp | publish | 64 B | 1 | 3,084 | 289 us | - | 18 MB |
| amqp | publish | 64 B | 128 | 3,047 | 39.3 ms | - | 20 MB |
| amqp | publish | 64 B | 256 | 3,183 | 75.7 ms | - | 20 MB |
| amqp | publish | 1 KiB | 1 | 1,947 | 477 us | - | 19 MB |
| amqp | publish | 1 KiB | 128 | 1,892 | 57.0 ms | - | 29 MB |
| amqp | publish | 1 KiB | 256 | 1,886 | 113 ms | - | 30 MB |
| amqp | consume | 64 B | 128 | 36,158 | - | 3.56 ms | 18 MB |
| amqp | consume | 64 B | 256 | 52,459 | - | 4.65 ms | 19 MB |
| amqp | consume | 1 KiB | 128 | 35,068 | - | 3.63 ms | 28 MB |
| amqp | consume | 1 KiB | 256 | 47,940 | - | 5.23 ms | 29 MB |
| amqp | both | 64 B | 128 | 2,803 | 42.3 ms | 42.6 ms | 18 MB |
| amqp | both | 1 KiB | 128 | 2,495 | 47.2 ms | 47.9 ms | 18 MB |

The 0-duplicate invariant held in all 72 runs. Publish-only runs verify the
stored queue after timing, consume/both runs verify every delivered payload,
and every run asserts `ready=0` and `inflight=0` at the end. An earlier harness
version faked the verifier in publish-only mode and pre-filled `both`; both
bugs are fixed, and the numbers above are from the corrected harness.

Reading:

- **Native batching is real end-to-end.** 64 B at batch 256 measured 76k
  jobs/s in this run (up to 85k on a quieter machine); 1 KiB at batch 128
  ~41k/s. The 1 KiB / 256 cell reported 31k as a full-suite median, but two
  of its three reps were slowed by background disk load; five targeted reruns
  landed at 51.7–55.7k. The loss versus the engine-only path (~52k/s at 1 KiB
  batch 128) is encoding, copying, and the socket buffer, not storage. Cells
  vary by ±20% between runs (more for 1 KiB / 256 under concurrent load) and
  should be read as ranges.
- **Buffering mattered enormously.** Before this phase, the RESP writer
  flushed every bulk element and the AMQP writer did five `write_all` calls per
  frame. At 1 KiB / batch 128, native was ~11k jobs/s. Buffering the writers
  and flushing once per reply lifted the same workload to ~41k jobs/s. This
  change is in `crates/ante-native` and `crates/ante-amqp`.
- **AMQP consumption is competitive; AMQP publishing is not.** Consume reaches
  35–52k jobs/s in this run (29–56k across runs; a take of `prefetch` messages
  + one `basic.ack multiple` is two commits per batch). Publish is 1.9–3.2k
  jobs/s and gets *worse* at larger
  batches because `basic.publish` has no batch form: every message is its own
  durable commit, and `wait_for_confirms` only waits for those commits. Batch
  256 therefore costs 256 commits, not one.
- **AMQP `both` is publish-bound**, landing at ~2.4–2.8k jobs/s here (1.8–3.2k
  across runs) even though the
  consumer could drain 50k/s. This is the single largest performance gap in the
  product.
- Native batch 1 is ~1.3–1.4k jobs/s, matching the per-message commit warning.
- Peak RSS is 8–19 MB for native and 18–30 MB for AMQP in this process (which
  also hosts the server, client, and lapin). The corrected `both` path no
  longer carries a pre-filled backlog, so its RSS dropped from the invalid 56 MB
  measurement to 18 MB.

### Consequence: AMQP publish group commit is the next performance project

The fix is to stop committing per `basic.publish`:

1. Decouple the connection reader from the commit: enqueue completed publishes
   into a per-connection batcher instead of awaiting the commit inline.
2. Batch requests for a short window (or up to 256 messages), group by queue,
   and call `publish_batch` once per group.
3. Send `basic.ack` confirms after the group commit, preserving the guarantee
   that a confirm means durable.

Until then, high-rate producers should use the native `PUBLISH_BATCH`, and
AMQP producers should treat ~2–3k messages/s as the ceiling. This is documented
in `docs/amqp-compatibility.md`.

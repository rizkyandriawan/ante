# ADR 0001: redb as the v1 storage backend

Date: 2026-10-23
Status: accepted

## Context

Ante needs a single-process, durable key/value store that can hold:

- message bodies (opaque bytes, typically 64 B–8 KiB),
- queue metadata,
- a ready index ordered by sequence,
- an in-flight index with lease deadlines.

Requirements, in order:

1. Correct recovery after `kill -9`.
2. Low latency for a durable single-message publish (~1–2k/s floor).
3. High throughput when commits are batched (target ≥50k jobs/s).
4. Bounded memory independent of backlog size.
5. Simple deployment: one process, one file, no server to operate.
6. A small dependency and build surface (Linux first, but portable).

The candidates were redb (pure Rust), LMDB via `heed`, SQLite, sled, and a
custom append-only log plus index.

## Decision

Use **redb 2.6** with immediate durability, a 64 MiB cache, and a narrow
storage module (`crates/ante-core/src/store.rs`). Do not let storage-specific
types leak into the engine API. Keep the door open for an LMDB backend if a
workload proves to need it.

## Evidence

Measured on a 13th Gen i7-13700H, 15.7 GiB RAM, ext4/NVMe. Full method and raw
data in `bench/RESULTS.md`.

| Metric | redb | LMDB (`heed` 0.22) |
|---|---:|---:|
| Durable commit, batch 1, 1 KiB | ~160 µs (~2.0k jobs/s) | ~290 µs (~1.1k jobs/s) |
| 1 KiB, batch 128 | ~88k jobs/s | ~110k jobs/s |
| 64 B, batch 128 | ~113k jobs/s | ~124k jobs/s |
| 8 KiB, batch 128 | ~22k jobs/s | ~32k jobs/s |
| 1M × 1 KiB: disk | 2.11 GB | 2.08 GB |
| 1M × 1 KiB: peak RSS | 66 MB | 972 MB |
| Idle RSS after open | 4 MB | 2 MB |
| Full scan of 1M jobs | 1.99 s | 0.32 s |
| `kill -9` cycles / invariant failures | 30 / 0 | 30 / 0 |

The decisive numbers are the single-message commit latency (redb ~1.8x faster)
and bounded memory. LMDB's mmap footprint is reclaimable page cache rather than
a leak, but it is still charged to the process/cgroup and dominates the
footprint of a service that claims to be small. LMDB wins on scans and on file
size for tiny payloads; neither is the primary Ante workload.

## Consequences

Positive:

- Pure Rust: one toolchain, no native library, no `map_size` tuning.
- Memory is predictable and small at rest and at a 1M-job backlog.
- Durable commits are fast enough for interactive publish-confirm use.

Negative:

- Full scans are ~6x slower than LMDB. Startup does not scan, so this only
  affects maintenance/export paths.
- Disk overhead for small messages is higher: 500k × 64 B is 94 MB vs 56 MB.
- Throughput at large payloads and large batches trails LMDB by 25–45%;
  group commit (batch 128–256) is required to reach the target range.
- redb takes an exclusive file lock. The CLI must be a client, not a second
  opener, and backup means stopping the server or snapshotting the filesystem.

Revisit if: full-scan latency or small-message file size becomes a product
requirement, or if redb's write amplification proves problematic on a target
filesystem. The storage boundary is intentionally narrow to make that swap
contained.

## Alternatives rejected

- **SQLite**: mature and portable, but durable single-row commits are in the
  same ~100–300 µs class, and the queue would be expressed as SQL with
  index/maintenance overhead. WAL mode plus `synchronous=FULL` does not offer a
  clear win, and the dependency is heavier.
- **sled**: unmaintained relative to the alternatives and its on-disk stability
  guarantees are weaker than required for a durability-first product.
- **Custom log + index**: maximum control, but recovery, compaction, and
  crash-consistency are exactly the parts that should not be hand-rolled for a
  v1.
- **LMDB**: better scans and smaller idle RSS, but the mmap memory profile and
  native build cost are a worse fit for "small daemon next to an app".
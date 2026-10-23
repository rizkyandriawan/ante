#!/usr/bin/env python3
"""Summarize Ante storage benchmark results (redb vs LMDB/heed)."""
import glob
import json
import statistics as st
import sys

BENCH = "/home/rizky/product/ante/bench"


def load():
    rows = []
    for path in sorted(glob.glob(f"{BENCH}/results/*.jsonl")):
        if "superseded" in path:
            continue
        for line in open(path):
            line = line.strip()
            if line:
                rows.append(json.loads(line))
    return rows


def mb(kb):
    return round(kb / 1024)


def table_lifecycle(rows):
    life = [r for r in rows if r["phase"] == "lifecycle"]
    print("### Lifecycle: durable enqueue -> claim -> ack (median of 3 reps)\n")
    print("| Payload | Batch | Backend | Durable jobs/s | Publish-confirm p50/msg | p99/msg | Claim p50/msg | Peak RSS |")
    print("|---:|---:|---|---:|---:|---:|---:|---:|")
    for p in (64, 1024, 8192):
        for b in (1, 32, 128):
            for be in ("redb", "lmdb-heed"):
                rs = [r for r in life if r["payload_bytes"] == p and r["batch"] == b and r["backend"] == be]
                if not rs:
                    continue
                ops = st.median(r["ops_per_sec"] for r in rs)
                e50 = st.median(r["enqueue_batch_us_p50"] for r in rs) / b
                e99 = st.median(r["enqueue_batch_us_p99"] for r in rs) / b
                c50 = st.median(r["claim_batch_us_p50"] for r in rs) / b
                hwm = st.median(r["hwm_kb"] for r in rs)
                print(f"| {p} B | {b} | {be} | {ops:,.0f} | {e50:.1f} us | {e99:.1f} us | {c50:.1f} us | {mb(hwm)} MB |")
    print()


def table_backlog(rows):
    print("### Backlog: enqueue N, then drain (batch 64)\n")
    print("| Payload | Jobs | Backend | Enqueue/s | Drain/s | RSS after enqueue | Peak RSS | Disk |")
    print("|---:|---:|---|---:|---:|---:|---:|---:|")
    for r in [r for r in rows if r["phase"] == "backlog_drain"]:
        print(f"| {r['payload_bytes']} B | {r['total']:,} | {r['backend']} | "
              f"{r['enqueue_ops_per_sec']:,.0f} | {r['ops_per_sec']:,.0f} | "
              f"{mb(r['rss_after_enqueue_kb'])} MB | {mb(r['hwm_kb'])} MB | "
              f"{r['disk_after_enqueue_bytes']/1e6:,.0f} MB |")
    print()


def table_bulk(rows):
    print("### Bulk enqueue, 1 KiB payload (median of 3 reps)\n")
    print("| Jobs | Backend | Enqueue/s | Disk | Peak RSS (fill) | Idle RSS after open |")
    print("|---:|---|---:|---:|---:|---:|")
    for total in (200_000, 1_000_000):
        for be in ("redb", "lmdb-heed"):
            fills = [r for r in rows if r["phase"] == "enqueue_only" and r["backend"] == be
                     and r["total"] == total and r.get("rep", 0) > 0]
            idles = [r for r in rows if r["phase"] == "idle_after_open" and r["backend"] == be
                     and r.get("rep", 0) > 0]
            if not fills:
                continue
            ops = st.median(r["ops_per_sec"] for r in fills)
            disk = st.median(r["db_alloc_bytes"] for r in fills)
            peak = st.median(r["hwm_kb"] for r in fills)
            idle = st.median(r["rss_kb"] for r in idles)
            print(f"| {total:,} | {be} | {ops:,.0f} | {disk/1e6:,.0f} MB | {mb(peak)} MB | {mb(idle)} MB |")
    print()


def table_recovery(rows):
    print("### Recovery: open + full integrity scan\n")
    print("| DB | Backend | Open | Integrity scan | RSS after scan |")
    print("|---|---|---:|---:|---:|")
    seen = set()
    for r in [r for r in rows if r["phase"] == "recover" and r.get("jobs", 0) >= 100_000]:
        key = (r["jobs"], r["backend"])
        if key in seen:
            continue
        seen.add(key)
        print(f"| {r['jobs']:,} jobs | {r['backend']} | {r.get('open_us', 0)/1000:.1f} ms | "
              f"{r.get('verify_us', 0)/1000:.0f} ms | {mb(r['rss_kb'])} MB |")
    print()


def table_retry(rows):
    print("### Retry-heavy: 20% of claimed jobs rescheduled (batch 64)\n")
    print("| Backend | State transitions/s | Peak RSS |")
    print("|---|---:|---:|")
    for r in [r for r in rows if r["phase"] == "retry_round"]:
        print(f"| {r['backend']} | {r['transitions_per_sec']:,.0f} | {mb(r['hwm_kb'])} MB |")
    print()


def table_crash(rows):
    crash = [r for r in rows if r["phase"] == "crash_recover"]
    print("### kill -9 recovery\n")
    print("| Backend | Cycles | Invariant failures | Duplicate live refs | Missing-job refs |")
    print("|---|---:|---:|---:|---:|")
    for be in ("redb", "lmdb-heed"):
        rs = [r for r in crash if r["backend"] == be]
        bad = sum(1 for r in rs if not r["invariants_ok"])
        dup = max((r["duplicate_live_references"] for r in rs), default=0)
        miss = max((r["references_to_missing_jobs"] for r in rs), default=0)
        print(f"| {be} | {len(rs)} | {bad} | {dup} | {miss} |")
    print()


if __name__ == "__main__":
    rows = load()
    print(f"Loaded {len(rows)} result rows\n")
    table_lifecycle(rows)
    table_backlog(rows)
    table_bulk(rows)
    table_recovery(rows)
    table_retry(rows)
    table_crash(rows)
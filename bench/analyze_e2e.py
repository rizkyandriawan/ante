#!/usr/bin/env python3
"""Summarize e2e JSONL results: median wall time and throughput per cell."""

import glob
import json
import statistics
import sys
from collections import defaultdict


def load(path):
    rows = []
    with open(path) as handle:
        for line in handle:
            line = line.strip()
            if line:
                rows.append(json.loads(line))
    return rows


def median(values):
    return statistics.median(values) if values else 0


def main():
    pattern = sys.argv[1] if len(sys.argv) > 1 else "results/*-e2e.jsonl"
    path = sorted(glob.glob(pattern))[-1]
    rows = load(path)
    groups = defaultdict(list)
    for row in rows:
        key = (row["protocol"], row["mode"], row["payload"], row["batch"])
        groups[key].append(row)

    print(f"source: {path} ({len(rows)} runs)\n")
    print("| protocol | mode | payload | batch | jobs/s (median) | wall ms | publish p50 us | consume p50 us | peak RSS MB |")
    print("|---|---|---:|---:|---:|---:|---:|---:|---:|")
    for key in sorted(groups, key=lambda k: (k[0], k[1], k[2], k[3])):
        protocol, mode, payload, batch = key
        cell = groups[key]
        print(
            f"| {protocol} | {mode} | {payload} | {batch} | "
            f"{median([r['jobs_per_sec'] for r in cell]):,.0f} | "
            f"{median([r['wall_ms'] for r in cell]):,.0f} | "
            f"{median([r['publish_batch_us_p50'] for r in cell]):,.0f} | "
            f"{median([r['consume_batch_us_p50'] for r in cell]):,.0f} | "
            f"{median([r['peak_rss_kb'] for r in cell]) / 1024:,.0f} |"
        )

    duplicates = sum(r.get("duplicates", 0) for r in rows)
    missing = [r for r in rows if r.get("jobs_per_sec", 0) <= 0]
    print(f"\nduplicate deliveries: {duplicates}; empty runs: {len(missing)}")


if __name__ == "__main__":
    main()

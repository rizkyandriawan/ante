import json
import os
import statistics
import subprocess
import sys
import tempfile
import threading
import time
from pathlib import Path

from ante import Ante

JOBS = 10000
PAYLOAD_SIZE = 1024
BATCH = 100
WORKERS = 16
GLOBAL_BUDGET_S = 120
SUPERVISOR_S = 125


def payload(index):
    return index.to_bytes(8, "big") + b"x" * (PAYLOAD_SIZE - 8)


def main():
    deadline = time.monotonic() + GLOBAL_BUDGET_S
    results = {}
    with tempfile.TemporaryDirectory() as directory:
        path = Path(directory) / "ante.sqlite3"
        with Ante.open(path) as ante:
            ante.declare("jobs")
            started = time.perf_counter()
            for batch_index in range(JOBS // BATCH):
                offset = batch_index * BATCH
                ante.publish_batch(
                    "jobs",
                    [payload(offset + index) for index in range(BATCH)],
                )
            results["publish_s"] = time.perf_counter() - started

            poll_latency = []
            poll_errors = []
            stop = threading.Event()

            def poll():
                try:
                    while not stop.is_set():
                        remaining = deadline - time.monotonic()
                        if remaining <= 0:
                            return
                        started = time.perf_counter()
                        ante.info("jobs")
                        poll_latency.append((time.perf_counter() - started) * 1000)
                        stop.wait(min(0.02, remaining))
                except Exception as error:  # noqa: BLE001
                    poll_errors.append(error)
                    stop.set()

            poller = threading.Thread(target=poll)
            poller.start()

            acked = [0] * WORKERS
            seen = set()
            tokens = set()
            errors = []
            lock = threading.Lock()

            def worker(worker_index):
                try:
                    while not stop.is_set():
                        remaining = deadline - time.monotonic()
                        if remaining <= 0:
                            return
                        wait_ms = max(1, min(20, int(remaining * 1000)))
                        deliveries = ante.take("jobs", max=32, wait_ms=wait_ms)
                        if not deliveries:
                            with lock:
                                if sum(acked) >= JOBS:
                                    return
                            continue
                        indexes = []
                        delivery_tokens = []
                        for delivery in deliveries:
                            index = int.from_bytes(delivery.raw[:8], "big")
                            if delivery.raw != payload(index):
                                raise AssertionError(f"payload mismatch for {index}")
                            indexes.append(index)
                            delivery_tokens.append(delivery.token)
                        outcomes = ante.ack(*delivery_tokens)
                        if len(outcomes) != len(delivery_tokens):
                            raise AssertionError(
                                f"ack count mismatch: {len(outcomes)} != {len(delivery_tokens)}"
                            )
                        if not all(outcome.ok for outcome in outcomes):
                            raise AssertionError(f"ack failed: {outcomes}")
                        with lock:
                            for index in indexes:
                                if index in seen:
                                    raise AssertionError(f"duplicate message {index}")
                                seen.add(index)
                            for token in delivery_tokens:
                                if token in tokens:
                                    raise AssertionError("duplicate delivery token")
                                tokens.add(token)
                            acked[worker_index] += len(deliveries)
                except Exception as error:  # noqa: BLE001
                    errors.append(error)
                    stop.set()

            started = time.perf_counter()
            threads = [
                threading.Thread(target=worker, args=(index,))
                for index in range(WORKERS)
            ]
            for thread in threads:
                thread.start()
            join_budget = max(1.0, deadline - time.monotonic() + 5)
            for thread in threads:
                thread.join(timeout=join_budget)
            if any(thread.is_alive() for thread in threads):
                stop.set()
                raise AssertionError("drain did not finish within the global budget")
            results["drain_s"] = time.perf_counter() - started
            stop.set()
            poller.join(timeout=5)
            if poller.is_alive():
                raise AssertionError("poller did not stop")

            if errors:
                raise errors[0]
            if poll_errors:
                raise poll_errors[0]

            results["poll_n"] = len(poll_latency)
            results["poll_p50_ms"] = statistics.median(poll_latency)
            ordered = sorted(poll_latency)
            results["poll_p95_ms"] = ordered[max(0, int(len(ordered) * 0.95) - 1)]
            results["poll_max_ms"] = max(poll_latency)
            results["acked"] = sum(acked)
            results["unique_messages"] = len(seen)
            results["unique_tokens"] = len(tokens)
            results["duplicates"] = sum(acked) - len(seen)
            results["seen_exact"] = seen == set(range(JOBS))
            results["total"] = ante.info("jobs")["total"]
            results["db_bytes"] = path.stat().st_size
            wal = Path(str(path) + "-wal")
            results["wal_bytes"] = wal.stat().st_size if wal.exists() else 0

    assert results["acked"] == JOBS, results
    assert results["unique_messages"] == JOBS, results
    assert results["seen_exact"], results
    assert results["duplicates"] == 0, results
    assert results["total"] == 0, results
    print(json.dumps(results, sort_keys=True))


if __name__ == "__main__":
    if os.environ.get("ANTE_PROBE_SUPERVISED") != "1":
        environment = os.environ.copy()
        environment["ANTE_PROBE_SUPERVISED"] = "1"
        try:
            completed = subprocess.run(
                [sys.executable, os.path.abspath(__file__)],
                check=False,
                timeout=SUPERVISOR_S,
                env=environment,
            )
        except subprocess.TimeoutExpired:
            raise SystemExit("probe exceeded the 125s supervisor limit") from None
        raise SystemExit(completed.returncode)
    main()

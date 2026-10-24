import json
import os
import signal
import subprocess
import sys
import tempfile
import time
from pathlib import Path

CHILD = r'''
import json, os, sys, time
phase, db, ready = sys.argv[1:4]
from ante import Ante

def emit(value):
    tmp = ready + ".tmp"
    with open(tmp, "w") as handle:
        json.dump(value, handle)
    os.replace(tmp, ready)

if phase == "publish":
    ante = Ante.open(db)
    ante.declare("jobs")
    ante.publish_batch("jobs", [f"publish-{i}".encode() for i in range(200)])
    emit({"published": 200})
elif phase == "inflight":
    ante = Ante.open(db)
    deliveries = ante.take("jobs", max=5, lease_ms=60_000)
    emit({"message_ids": [delivery.message_id for delivery in deliveries]})
elif phase == "acked":
    ante = Ante.open(db)
    delivery = ante.take("jobs", max=1, lease_ms=60_000)[0]
    delivery.ack()
    emit({"message_id": delivery.message_id})
elif phase == "complete":
    ante = Ante.open(db)
    delivery = ante.take("jobs", max=1, lease_ms=60_000)[0]
    result_id = delivery.complete("results", b"result")
    emit({"message_id": delivery.message_id, "result_id": result_id})
elif phase == "lock":
    ante = Ante.open(db)
    ante.declare("jobs")
    emit({"opened": True})
else:
    raise SystemExit(f"unknown phase {phase}")
time.sleep(120)
'''

def prepare(path):
    from ante import Ante
    with Ante.open(path) as ante:
        ante.declare("jobs")
        ante.publish_batch("jobs", [f"job-{i}".encode() for i in range(5)])
        ante.declare("results")

def child(phase, path, work):
    ready = Path(work) / f"{phase}.ready"
    log_path = Path(work) / f"{phase}.log"
    with log_path.open("wb") as log:
        process = subprocess.Popen(
            [sys.executable, "-c", CHILD, phase, str(path), str(ready)],
            stdout=log,
            stderr=subprocess.STDOUT,
        )
    deadline = time.monotonic() + 30
    while not ready.exists():
        if process.poll() is not None:
            raise RuntimeError(f"{phase} exited before ready: {log_path.read_text()}")
        if time.monotonic() > deadline:
            process.kill()
            process.wait()
            raise RuntimeError(f"{phase} did not become ready")
        time.sleep(0.005)
    data = json.loads(ready.read_text())
    return process, data

def kill(process):
    os.kill(process.pid, signal.SIGKILL)
    returncode = process.wait(timeout=10)
    assert returncode == -signal.SIGKILL, returncode


def ack_all(ante, tokens):
    outcomes = ante.ack(*tokens)
    assert len(outcomes) == len(tokens), (len(outcomes), len(tokens))
    assert all(outcome.ok for outcome in outcomes), outcomes
    return outcomes

def check(work, phase, data):
    from ante import Ante
    path = Path(work) / "ante.sqlite3"
    if phase == "publish":
        with Ante.open(path) as ante:
            info = ante.info("jobs")
            assert info["ready"] == 200 and info["inflight"] == 0, info
            deliveries = ante.take("jobs", max=200)
            assert len(deliveries) == 200
            assert {
                (delivery.message_id, delivery.raw) for delivery in deliveries
            } == {
                (index, f"publish-{index - 1}".encode())
                for index in range(1, 201)
            }
            ack_all(ante, [delivery.token for delivery in deliveries])
            assert ante.info("jobs")["total"] == 0
    elif phase == "inflight":
        with Ante.open(path) as ante:
            info = ante.info("jobs")
            assert info["ready"] == 5 and info["inflight"] == 0, info
            message_ids = set(data["message_ids"])
            assert len(message_ids) == 5, data
            for message_id in message_ids:
                status = ante.message_status("jobs", message_id)
                assert status["state"] == "ready", status
                assert status["history"][-1]["outcome"] == "recovered", status
            deliveries = ante.take("jobs", max=5)
            assert {
                (delivery.message_id, delivery.raw) for delivery in deliveries
            } == {
                (message_id, f"job-{message_id - 1}".encode())
                for message_id in message_ids
            }
            ack_all(ante, [delivery.token for delivery in deliveries])
    elif phase == "acked":
        with Ante.open(path) as ante:
            info = ante.info("jobs")
            assert info["ready"] == 4 and info["inflight"] == 0, info
            acked = ante.message_status("jobs", data["message_id"])
            assert acked["state"] == "acked", acked
            deliveries = ante.take("jobs", max=10)
            expected = set(range(1, 6)) - {data["message_id"]}
            assert {
                (delivery.message_id, delivery.raw) for delivery in deliveries
            } == {
                (message_id, f"job-{message_id - 1}".encode())
                for message_id in expected
            }
            ack_all(ante, [delivery.token for delivery in deliveries])
    elif phase == "complete":
        with Ante.open(path) as ante:
            info = ante.info("jobs")
            assert info["ready"] == 4 and info["inflight"] == 0, info
            input_status = ante.message_status("jobs", data["message_id"])
            assert input_status["state"] == "acked", input_status
            remaining = ante.take("jobs", max=10)
            expected = set(range(1, 6)) - {data["message_id"]}
            assert {
                (delivery.message_id, delivery.raw) for delivery in remaining
            } == {
                (message_id, f"job-{message_id - 1}".encode())
                for message_id in expected
            }
            ack_all(ante, [delivery.token for delivery in remaining])
            results = ante.info("results")
            assert results["ready"] == 1 and results["inflight"] == 0, results
            deliveries = ante.take("results", max=1)
            assert len(deliveries) == 1
            assert deliveries[0].message_id == data["result_id"], data
            assert deliveries[0].raw == b"result"
            ack_all(ante, [deliveries[0].token])
    elif phase == "lock":
        with Ante.open(path) as ante:
            assert ante.queue_exists("jobs")
    else:
        raise AssertionError(phase)

def run(phase, repetition):
    process = None
    try:
        with tempfile.TemporaryDirectory() as work:
            path = Path(work) / "ante.sqlite3"
            if phase in {"inflight", "acked", "complete"}:
                prepare(path)
            process, data = child(phase, path, work)
            if phase == "lock":
                from ante import Ante, AnteError
                failed = False
                try:
                    second = Ante.open(path)
                except (AnteError, OSError):
                    failed = True
                else:
                    second.close()
                if not failed:
                    raise AssertionError("second owner opened the database")
            kill(process)
            check(work, phase, data)
    finally:
        if process is not None and process.poll() is None:
            kill(process)

def main():
    phases = ["publish", "inflight", "acked", "complete", "lock"]
    for phase in phases:
        for repetition in range(3):
            run(phase, repetition)
        print(f"{phase}: 3/3 passed", flush=True)

if __name__ == "__main__":
    main()

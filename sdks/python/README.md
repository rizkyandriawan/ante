# Ante for Python

Embed the Ante queue daemon in a Python application. The SDK starts the
`ante` binary, waits for its native endpoint, and speaks the RESP2 native
protocol. If you already have a daemon, use `Ante.connect()` instead.

Requires Python 3.9+ and the `ante` binary (set `ANTE_BIN` or pass
`binary="/path/to/ante"`).

## Install

```bash
pip install -e sdks/python
```

## Consume

If you know `pika`, this is the same shape:

```python
from ante import Ante

ante = Ante.open("./data/ante.redb")

def handle(job, delivery):
    resize(job)
    # returning acknowledges; delivery.ack() / delivery.requeue() also work

consumer = ante.consume("jobs", handle, prefetch=16)
```

`consume` returns immediately and runs handlers on a thread pool. Call
`consumer.stop()` to stop taking new work and wait for active handlers. For a
blocking worker:

```python
ante.run("jobs", handle, prefetch=16)  # Ctrl-C stops it
```

Handlers receive `(payload, delivery)`. `payload` is decoded from JSON, text, or
bytes based on the content type. `delivery` exposes `token`, `message_id`,
`attempts`, `content_type`, and `raw`.

A handler is acknowledged after it returns. If it raises, the default policy
requeues the message up to `max_attempts` (default 5) and then drops it. Pass
`on_error` to decide per message:

```python
consumer = ante.consume(
    "jobs",
    handle,
    prefetch=16,
    max_attempts=None,  # never drop automatically
    on_error=lambda error, delivery: "requeue" if retryable(error) else "drop",
)
```

`on_error` returns `"requeue"` or `"drop"`. A dropped message is acknowledged
(removed). Requeue increments `delivery.attempts`; use it for retry limits.
A handler that runs longer than `lease_ms` can be redelivered; keep handlers
bounded or raise the lease.

## Publish

```python
message_id = ante.publish("jobs", {"task": "resize", "id": 42})
ids = ante.publish_batch("jobs", [{"id": 1}, {"id": 2}, {"id": 3}])
```

Objects become JSON, strings become text, and bytes are sent as
`application/octet-stream`. `publish` returns after the durable commit.
`publish_batch` is one commit for up to 256 JSON/text payloads.

## Manual take / ack

```python
[delivery] = ante.take("jobs", max=1, lease_ms=30_000, wait_ms=5_000)
try:
    handle(delivery.payload, delivery)
    delivery.ack()
except Exception:
    delivery.requeue()
```

`take` is the low-level primitive used by `consume`; `wait_ms` long-polls.
`ack` accepts multiple tokens and returns per-token `AckResult`s, because a
stale or unknown token does not fail the rest of the batch.

## API

| Method | Description |
|---|---|
| `Ante.open(db, binary=None)` | Start and own a daemon; creates the DB directory |
| `Ante.connect(host, port)` | Use an existing daemon (default `127.0.0.1:7300`) |
| `declare(queue)` | Idempotent queue declaration |
| `publish(queue, payload, content_type=None, message_id=None)` | Durable publish |
| `publish_batch(queue, payloads)` | One durable commit |
| `take(queue, max=1, lease_ms=30_000, wait_ms=0)` | Take deliveries |
| `ack(*tokens)` / `requeue(token)` | Settle deliveries |
| `consume(queue, handler, **options)` | Start a threaded consumer |
| `run(queue, handler, **options)` | Blocking consumer until Ctrl-C |
| `info(queue)` / `list_queues()` | Ready and in-flight counts |
| `purge(queue)` / `delete_queue(queue)` | Queue maintenance |
| `close()` | Stop consumers, close sockets, stop the daemon |

`Ante` and `Consumer` support context managers. `close()` stops every consumer
created by that `Ante` instance.

## Threading model

- One control connection carries `PUBLISH`, `ACK`, `REQUEUE`, and `INFO`;
  calls are serialized with a lock, so the client is safe to share between
  threads.
- Each consumer opens a separate connection for long-polling `TAKE`, so an
  idle poll never delays an acknowledgement.
- Handlers run on a `ThreadPoolExecutor` sized to `prefetch`. `prefetch` is
  also the maximum number of unacknowledged deliveries.
- Do not call `consumer.stop()` or `ante.close()` from inside a handler; it
  waits for the handler to return.

This is a synchronous SDK. For asyncio, run handlers with
`asyncio.run_coroutine_threadsafe` or use `asyncio.to_thread`; a native async
client is not included yet.

## Tests

```bash
cd sdks/python
PYTHONPATH=src python3 -m unittest discover -s tests -v
```

The smoke tests spawn the real daemon from `target/release/ante` (override with
`ANTE_BIN`).
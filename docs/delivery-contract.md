# Delivery contract

This document is normative for both endpoints (native and AMQP). If the code
and this document disagree, that is a bug in one of them.

## Guarantee

Ante provides **at-least-once** delivery with durable storage:

1. A publish reply is sent only after the message is durably committed.
2. A message is removed only after a successful `ack`, which is itself durably
   committed before the reply.
3. A crash, connection loss, or lease expiry can cause a message to be
   delivered again. It cannot cause a message that was successfully published
   and not acked to disappear.

Handlers must therefore be idempotent. Ante does not deduplicate.

There is no exactly-once mode and no transactional handoff between the queue
and application state. If a handler writes to a database and then acks, a crash
between those steps redelivers the message; if it acks first and then writes, a
crash loses the work. The standard fix is an idempotency key: use the
`message_id` (AMQP basic property or native message id) as a unique key in the
application's write.

## State machine

```
              publish
                 |
                 v
   +---------- ready <---------+
   |             |             |
   |             | take        | requeue / lease expiry /
   |             v             | process restart
   |         in-flight --------+
   |             |
   |             | ack
   |             v
   +---------- deleted
      purge / delete queue
```

- `ready`: durable and eligible for delivery.
- `in-flight`: leased to one consumer. Still stored; not eligible until the
  lease expires or it is requeued.
- `deleted`: removed by `ack`, `purge`, or `delete queue`.

`take`/`basic.get` is a durable state transition, not a read. An in-flight
message survives a process crash and is recovered on the next start.

## Queue lifecycle

A queue name is reusable, but every create produces a new identity. Consumers
are bound to the identity they subscribed to:

- Deleting a queue ends its consumers. They do not silently attach to a queue
  recreated with the same name. AMQP clients that advertised
  `consumer_cancel_notify` receive exactly one `basic.cancel` after the last
  in-flight delivery; the tag becomes reusable once that notification is
  written. The same applies when the queue is deleted through the native API.
- AMQP `queue.delete` honors `if_empty` and `if_unused`; a rejected delete
  leaves the queue and its messages untouched.
- Consumer tags are unique per channel. A duplicate is rejected rather than
  replacing the earlier consumer.

On server shutdown the listener stops accepting, active connections are
stopped, consumers are ended, and their in-flight deliveries are requeued.
Aborted consumer tasks are handed to a server-level reaper and joined, not just
signalled, so the server does not return while a consumer is still running.
In-flight storage work is tracked and gets the same 5-second shutdown budget. If that budget is
exhausted, the server returns anyway: commits are atomic (they either completed
or never happened), and any message still leased is recovered by lease expiry
after restart.

## Leases

- Default lease: 30 seconds. Native clients may override per `TAKE`
  (`lease-ms`). AMQP clients cannot set a lease; the server ACK timeout default of 30 minutes applies.
- The lease starts when the delivery is handed to the consumer (after the take
  commit).
- On expiry the message returns to ready and its attempt count increments.
- Recovery runs once per second in the server process, and once at startup for
  all in-flight messages left by a previous process.
- AMQP connections have a 30-second heartbeat by default. A silent peer is
  disconnected after two intervals, and that connection's in-flight deliveries
  are requeued immediately by channel cleanup instead of waiting for lease
  expiry.

A lease is not a visibility timeout that can be extended. If a handler needs
more time, it must be fast enough, use a shorter unit of work, or (in v1) ack
and re-publish. Lease extension is a candidate for a later version.

## Attempt history

The engine records one attempt row per successful take: attempt number,
delivery id, start time, finish time, and final outcome. The final outcome is
`acked`, `requeued`, `expired`, `recovered` after a process restart, or
`discarded` when it is explicitly dropped or its queue is deleted. History survives ack and queue deletion;
read it with the native `STATUS` command or the embedded `message_status` API.
History is diagnostic metadata, not a delivery guarantee: the queue entries,
not the history table, remain the source of truth for pending work.

## Tokens and staleness

Every delivery gets a token (`<message-id>:<delivery-id>` natively; the
delivery tag in AMQP maps to the same in-flight record). The token identifies
one delivery attempt, not just the message.

- If a message is redelivered after a lease expiry, the old token is **stale**.
  Acking it returns `STALE_DELIVERY` and does not remove the newer delivery.
  This prevents a slow worker from deleting work that another worker now owns.
- If the delivery is already gone (acked, purged, queue deleted), the token is
  **unknown**. Acking it returns `UNKNOWN_DELIVERY`.
- Stale and unknown acks are normal under at-least-once. Workers should log
  them, not crash.

## Duplicate delivery scenarios

| Scenario | Result |
|---|---|
| Consumer crashes before ack | Redelivered after lease expiry or restart |
| Consumer is slow; lease expires | Redelivered; first consumer's ack is stale |
| Publish commit succeeds, reply lost | Producer may retry and create a duplicate |
| Ack commit succeeds, reply lost | Message is gone; retried ack is unknown |
| Two consumers race on the same queue | Each message is delivered to exactly one at a time; no duplicate from the queue itself |
| `basic.reject`/`basic.nack` with requeue | Redelivered, attempt count increments |
| Queue purge/delete while in-flight | Delivery becomes unknown; ack has no effect |

There is no scenario in which the queue itself delivers one message to two
consumers simultaneously. Duplicates come from redelivery, not fan-out.

## Attempts

- First delivery: `attempts = 1`.
- Every requeue or lease expiry increments it.
- Native `TAKE` exposes it as field 5.
- AMQP sets `redelivered = attempts > 1`. AMQP does not expose the count.
- There is no maximum attempt count and no dead-letter queue in v1. A poison
  message will be redelivered forever unless a worker acks it (discarding the
  work) or deletes/purges the queue. Production workers should implement their
  own attempt budget in the payload and ack when exhausted.

## Ordering

Messages are delivered in publish order (FIFO by sequence) within a queue.
Requeued messages go to the back of the ready set. With multiple consumers,
completion order is not guaranteed. Ante does not provide priority or
scheduling.

**Fairness is not guaranteed.** A consumer takes a batch at a time, and a fast
consumer can take most of the backlog. Prefetch bounds how much one AMQP
channel holds unacked, and native `TAKE max` bounds one request. If fairness
between workers matters, use prefetch/`max` of 1 and accept the throughput
cost. The stress suite verifies exactly-once queue ownership and no loss under
concurrency, not equal distribution.

## Durability details

- SQLite runs in WAL mode with `synchronous=FULL`. Each publish, take, ack,
  requeue, purge, and delete is a transaction; the reply is sent after commit
  returns.
- `PUBLISH_BATCH` and multi-token `ACK` are single transactions: all or
  nothing.
- The database is a single file. Copying it while the server is running is not
  a supported backup method; stop the server or use a filesystem snapshot.
- Crash testing used `kill -9` (30 cycles per backend) with full reference
  integrity checks: zero failures. This proves process-crash safety, not
  power-loss safety; that depends on the filesystem and device honoring flush.
- Ante takes an exclusive owner lock next to the SQLite file. A second process
  cannot open the same database; the CLI is a client, not a second opener.

## Backpressure

- Native `TAKE max` is capped at 256. `PUBLISH_BATCH` is capped at 256 messages
  and 1 MiB.
- AMQP `basic.qos(prefetch_count=N)` limits unacked deliveries per channel.
  `prefetch_count=0` is treated as the default 32. The limit is per channel,
  not per consumer.
- Consumers without prefetch limits can accumulate an unbounded in-flight set
  in memory (the messages are already durable). Set a prefetch.

## Acknowledgment batching

Throughput depends on batching. Three durable commits per message (publish,
take, ack) cap a single-threaded worker at roughly 1–2k jobs/s. Use:

- Native `PUBLISH_BATCH` for producers, and multi-token `ACK` for consumers.
- AMQP `basic.ack multiple=true` for consumers. For producers, AMQP has no
  batch publish: each `basic.publish` is one durable commit, so confirms
  pipeline round trips but do not group commits. The measured AMQP publish
  ceiling is ~2–3k messages/s; use native `PUBLISH_BATCH` above that.

End-to-end over loopback TCP (3 reps, median) with the real server:

| Path | 1 KiB throughput |
|---|---:|
| Native, batch 256 | ~38–46k jobs/s |
| Native, batch 128 | ~37–42k jobs/s |
| Native, batch 1 | ~1.2–1.4k jobs/s |
| AMQP consume, prefetch 256 | ~48k jobs/s |
| AMQP publish, confirm per message | ~2.1k messages/s |

The storage-only prototype reaches ~88k jobs/s at batch 128; the gap is wire
encoding, copying, and socket buffering. Batch size 128–256 is the sweet spot.

## Endpoint mapping

| Concept | Native | AMQP |
|---|---|---|
| Queue create | `DECLARE q` | `queue.declare` |
| Publish | `PUBLISH q payload` | `basic.publish "" q` |
| Publish confirm | `+<message-id>` after commit | `basic.ack` (confirm mode) |
| Claim | `TAKE q n lease wait` | `basic.consume` / `basic.get` |
| Ack | `ACK token...` | `basic.ack` |
| Requeue | `REQUEUE token` | `basic.reject/nack requeue=true` |
| Discard | `ACK token` | `basic.reject/nack requeue=false` |
| Purge | `PURGE q` | `queue.purge` |
| Delete | `DELETE q` | `queue.delete` |
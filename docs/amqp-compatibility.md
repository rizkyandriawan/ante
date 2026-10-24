# AMQP 0-9-1 compatibility

Ante exposes an AMQP 0-9-1 endpoint so workers can use existing client
libraries. It is a deliberately small profile: durable queues, the default
exchange, publish/consume, confirms, and manual acknowledgements.

Default address: `amqp://guest:guest@127.0.0.1:5672/%2f`.

## What works

| Area | Methods |
|---|---|
| Connection | `connection.start/start-ok`, `tune/tune-ok`, `open/open-ok`, `close/close-ok`, heartbeat frames |
| Channel | `channel.open/open-ok`, `close/close-ok`, `flow/flow-ok` |
| Queue | `queue.declare/declare-ok`, `bind/bind-ok`, `unbind/unbind-ok` (default exchange only), `purge/purge-ok`, `delete/delete-ok` |
| Basic | `qos/qos-ok`, `consume/consume-ok`, `cancel/cancel-ok`, `publish`, `get/get-ok/get-empty`, `ack`, `reject`, `nack`, `recover/recover-ok`, `deliver` |
| Confirms | `confirm.select/select-ok`, server `basic.ack`/`basic.nack` |
| Returns | `basic.return` for mandatory unroutable publishes |

Negotiated values:

- `channel_max = 16`
- `frame_max = 131072`
- `heartbeat = 30` seconds (negotiated)

The client's `tune-ok` values are honored: a lower `frame_max` (minimum 4096)
shrinks outgoing body frames, and a lower `channel_max` is enforced when a
channel is opened. `frame_max` is the total frame size, including the 8-byte
header/terminator overhead; inbound frames larger than it are rejected.
Exceeding `channel_max` is a connection error 504 `CHANNEL_ERROR`.

Heartbeats follow the AMQP negotiation rule: the smaller non-zero value wins;
if one side advertises `0`, the other side's value is used. The server sends a
heartbeat every half interval and closes a connection that has been silent for
two intervals with connection error 320 `CONNECTION_FORCED`. Heartbeat frames
keep an idle connection alive but do not acknowledge deliveries.

In the legacy (no `--security`) profile, `PLAIN` and `AMQPLAIN` are offered but
credentials and the virtual host are ignored. In the secure profile, only
`PLAIN` is offered; credentials are verified against Argon2id hashes, the
virtual host must be `/`, and every operation is checked against queue-scoped
permissions. See [security.md](security.md). Exposure without TLS requires an
explicit decision to run the development profile; run Ante on loopback or behind
a trusted TLS proxy.

## Semantics

- The default exchange `""` routes by routing key to a queue of the same name.
  There are no bindings to configure.
- `queue.declare` is idempotent. A non-passive declare creates the queue.
  The declare-ok reply reports the ready message count and the number of
  consumers bound to that queue identity.
  `passive=true` returns 404 `NOT_FOUND` if the queue does not exist.
- `durable`, `exclusive`, and `auto-delete` bits are accepted but not enforced.
  Queue lifetime is explicit (`queue.delete`); an exclusive queue is not
  removed when its connection closes.
- `queue.delete` honors `if_empty` and `if_unused`. A failed condition is a 406
  channel error and deletes nothing. `if_unused` sees consumers on **every**
  connection, matched by the queue's identity: a consumer still winding down
  from a deleted queue does not block the recreated queue. Registration is
  serialized against delete for that queue name.
- Deleting a queue ends every consumer bound to it, including consumers owned
  by other connections. A queue recreated with the same name has a new identity
  and is not served to the old consumers; the identity check and the take are
  one transaction. Outstanding deliveries for the deleted identity are dropped
  from the channel's prefetch accounting, so a resubscribe on the same channel
  can take new work immediately instead of waiting for the old lease.
- `basic.publish` is asynchronous. A message is durable when its
  `basic.ack` confirm arrives. Without confirm mode, the client has no
  durability signal.
- Publishing to a missing queue:
  - `mandatory=true`: the server sends `basic.return` (312 `NO_ROUTE`) with the
    original properties and body, then confirms the publish.
  - `mandatory=false`: the message is dropped, then confirmed.
- Unroutable publishes are still confirmed because the broker accepted them;
  `basic.return` is the routing signal.
- `basic.consume` starts a push consumer. Consumer tags are unique per channel;
  a duplicate tag is rejected with 530 `NOT_ALLOWED` and closes the channel.
  `basic.get` performs one synchronous take. `basic.get` with `no_ack=true`
  acks immediately.
- `basic.qos(prefetch_count=N)` limits unacked deliveries **per channel**
  (shared by all consumers on that channel). Capacity is reserved atomically
  before a take, so multiple consumers on one channel cannot exceed the limit.
  `prefetch_count=0` means the server default of 32. The prefetch size field is
  ignored.
- Deliveries use the server ACK timeout lease (30 minutes by default). An expired delivery is
  returned to ready and its prefetch slot is released. A late `basic.ack` for
  that expired tag is an unknown tag and closes the channel with 406
  `PRECONDITION_FAILED`; the redelivered message is unaffected.
- `basic.get` uses the same server lease as `basic.consume`. The take and the
  outstanding registration are serialized with deletion of that queue name, so
  a concurrent `queue.delete` cannot be followed by a stale delivery tag.
- `basic.ack` supports `multiple=true`. A non-zero tag that is not outstanding
  is a 406 `PRECONDITION_FAILED` channel error; `tag=0` with `multiple=true`
  acks all outstanding deliveries and is always valid.
- `basic.reject` and `basic.nack` support `requeue`. `requeue=false` is a
  permanent discard (equivalent to ack).
- `basic.recover` always requeues the channel's outstanding deliveries; the
  `requeue` argument is ignored.
- `basic.cancel` stops future deliveries; already-unacked deliveries stay
  in-flight until acked/requeued or the lease expires. The server waits for an
  in-flight delivery batch before acknowledging the cancel, so no delivery is
  written after the reply.
- Deleting a queue sends `basic.cancel` (nowait) to every consumer bound to it,
  after that consumer's `consume-ok` and after its current delivery batch. The
  notification is sent only when the client advertised
  `capabilities.consumer_cancel_notify`; otherwise the consumer is stopped
  silently. The tag is released only after the notification is written, so a
  tag reused by a new subscription is never cancelled by the old one. The
  client is expected to resubscribe if it still wants work from a recreated
  queue.
- A queue deleted through the core/native endpoint takes the same cancellation
  path: AMQP consumers on it are stopped and notified (capability permitting).
  Because that path has no registry event, a channel also reconciles its
  outstanding tags against the queue identity before treating a full prefetch
  window as real (at most every 500 ms). Outstanding without an active
  consumer (a held `basic.get`, or a cancelled consumer) therefore cannot
  block a new subscription on a recreated queue until the old lease expires.
  Cleanup only waits on channels that hold tags for the deleted queue identity;
  a stalled writer on an unrelated queue cannot delay `queue.delete`. Channel
  reservations and reconciliation are tied to a channel generation, so a
  closed and reopened channel number is never mutated by an old task.
- A connection close requeues all in-flight deliveries owned by that
  connection.
- `redelivered` is set when the delivery attempt count is greater than 1.
- `message_id`, `content_type`, `correlation_id`, and `reply_to` properties are
  stored and returned. Other properties are accepted and dropped.

## What is not supported

| Feature | Behavior |
|---|---|
| Named exchanges (`exchange.declare`) | Channel error 540 `NOT_IMPLEMENTED` |
| `exchange.delete` / `exchange.bind` | No-op success (no named exchanges exist) |
| Server-named queues (`queue.declare` with empty name) | Channel error 540 `NOT_IMPLEMENTED` |
| Transactions (`tx.select`, `tx.commit`, `tx.rollback`) | Channel error 540 `NOT_IMPLEMENTED` |
| mTLS, SASL mechanisms beyond PLAIN in secure mode | Not implemented |
| Named users / ACLs in legacy mode | Not implemented; use `--security` |
| `immediate` publish flag | Ignored |
| Priority, TTL, dead-letter, max-length arguments | Ignored |
| `basic.return` from client to server | Not a client method; rejected |
| Connection blocked/unblocked notifications | Not implemented |

Clients that declare a non-default exchange or use `tx.*` will fail or behave
differently. Most worker libraries only need queue declare, qos, consume,
publish, and ack, which are covered.

## Client configuration notes

**Python (pika)**

```python
params = pika.ConnectionParameters("127.0.0.1", heartbeat=60)
conn = pika.BlockingConnection(params)
ch = conn.channel()
ch.queue_declare(queue="jobs", durable=True)
ch.basic_qos(prefetch_count=16)
```

Pika's default heartbeat works. The server advertises 30 seconds and the
negotiated interval is the smaller non-zero value.

**Node (amqplib)**

```js
const conn = await amqp.connect("amqp://127.0.0.1");
const ch = await conn.createChannel();
await ch.assertQueue("jobs", { durable: true });
await ch.prefetch(16);
```

**Go (amqp091-go)**

```go
conn, _ := amqp.Dial("amqp://guest:guest@127.0.0.1:5672/")
ch, _ := conn.Channel()
ch.QueueDeclare("jobs", true, false, false, false, nil)
ch.Qos(16, 0, false)
```

## Confirms and batching

Enable confirm mode on producer channels. Ante confirms after the durable
commit, so a `basic.ack` confirm is a real durability signal.

Confirms do **not** group commits. `basic.publish` has no batch form: each
message is its own durable transaction, and `wait_for_confirms` only waits for
those transactions. Multiple outstanding confirms pipeline the round trips but
do not reduce the number of commits. Measured on the reference machine
(`bench/RESULTS.md`, section 8):

| Path | Throughput |
|---|---:|
| AMQP publish, per-message confirm | ~1.6–3.6k messages/s |
| AMQP consume, prefetch 256 + ack multiple | ~29–56k jobs/s |
| Native `PUBLISH_BATCH`, batch 256 | ~38–46k jobs/s (1 KiB) |

If a producer needs more than a few thousand messages per second, use the
native protocol's `PUBLISH_BATCH`, or split the work so that AMQP publishing is
not the bottleneck. Publish group commit is the next planned performance work;
until it lands, ~2–3k messages/s is the AMQP ceiling and is a documented
limitation, not a tuning problem.

Consumers should ack in batches (`multiple=true`) where the client supports it.
One ack per delivery is a durable commit per message and is the slow path;
`basic.ack multiple` turns a batch of deliveries into one commit.

## Error mapping

| Condition | AMQP result |
|---|---|
| Unknown queue on `queue.declare` (passive) | Channel close 404 `NOT_FOUND` |
| Unknown queue on `basic.consume`/`basic.get` | Channel close 404 `NOT_FOUND` |
| Payload over 8 MiB | Channel close 406 `PRECONDITION_FAILED` |
| Named exchange | Channel close 540 `NOT_IMPLEMENTED` |
| Unknown delivery tag in ack/reject/nack | Channel close 406 `PRECONDITION_FAILED` |
| Duplicate consumer tag | Channel close 530 `NOT_ALLOWED` |
| `queue.delete` fails `if_empty` or `if_unused` | Channel close 406 `PRECONDITION_FAILED` |
| Channel number above negotiated `channel_max` | Connection close 504 `CHANNEL_ERROR` |
| `channel.open` for an already-open channel | Connection close 504 `CHANNEL_ERROR` |
| Body frame exceeds the declared content size | Connection close 501 `FRAME_ERROR`; the connection is terminated |
| Malformed frame | Connection is closed (no close frame is sent) |
| Unexpected content frame | Connection close 505 `UNEXPECTED_FRAME` |

After a channel error the server sends `channel.close` and discards that
channel's state. The connection remains usable; the client must reopen the
channel.
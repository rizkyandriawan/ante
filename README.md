# Ante

A small, durable work queue for putting background jobs behind existing apps.

Ante is one binary and one database file. It speaks a tiny native protocol, an
AMQP 0-9-1 subset, and an opt-in AMQP 1.0 endpoint, so a worker can be written
in any language that already has an AMQP client, and a new service can use the
simpler native protocol without a client library.

Status: **v0.1, pre-1.0**. Single node, Linux-first. TLS, Argon2id
credentials, and queue-scoped permissions are available behind
`--security`; the default profile remains loopback-only plaintext. The file
format and protocol are expected to change before 1.0.

## Why another queue

Most queues are either a service you must operate (broker, cluster, dashboard)
or a library you must embed (then each process needs its own store). Ante aims
at the middle: an application embeds or ships one daemon, all workers share one
durable file, and the operational surface is `ante serve` plus a backup copy of
that file.

Design constraints:

- **Durability is the default.** A publish is acknowledged only after the
  commit is durable. There is no `NOSYNC` fast path.
- **At-least-once delivery.** Messages are never silently lost on a crash;
  handlers must be idempotent.
- **Queue identity.** Deleting a queue ends its consumers; recreating the name
  does not feed work to the old subscription. Conditional deletes are atomic.
- **Visible history.** Every take records an attempt; ack, discard, requeue,
  expiry, and recovery finish it with a timestamped outcome. Native `STATUS`
  reads one message without scanning ready work. Finished history is bounded by
  age and row count, and unfinished attempts are never pruned.
- **Atomic completion.** A worker can publish its result and settle its input
  in one transaction, removing the publish-then-crash duplicate window.
- **Renewable leases.** Native and embedded workers can extend an in-flight
  lease without creating a new attempt.
- **Batching is the throughput lever.** Sequential durable commits cost
  ~160 µs each; batch publish/ack/take amortizes that to a few µs per job.
  The native protocol exposes batching; AMQP production is limited by
  one-commit-per-publish (see Benchmarks).
- **Bounded memory.** Storage is SQLite in WAL mode with a bounded cache; a
  large backlog does not inflate RSS without limit.
- **Small compatibility surface.** The AMQP endpoint implements what a worker
  client needs, and rejects what it does not.

## Quick start

```bash
cargo build --release
./target/release/ante serve --db ./ante.sqlite3
```

For TLS and credentials, see [docs/security.md](docs/security.md):

```bash
ante serve --db ./ante.sqlite3 --security ./security.json
```

The default (development) listener is loopback-only and unauthenticated. The daemon listens on:

- native protocol: `127.0.0.1:7300`
- AMQP 0-9-1: `127.0.0.1:5672`
- AMQP 1.0: opt-in with `--amqp1 ADDR` (default `127.0.0.1:5673`)

CLI (talks to the native endpoint):

```bash
ante declare jobs
ante publish jobs '{"task":"resize","id":42}' --content-type application/json
ante info jobs
ante take jobs --max 1 --lease-ms 30000
# token <TAB> message_id <TAB> content_type ... <TAB> payload
ante ack <token>
```

AMQP worker (any client, e.g. Python `pika`):

```python
import pika
conn = pika.BlockingConnection(pika.ConnectionParameters("127.0.0.1"))
ch = conn.channel()
ch.queue_declare(queue="jobs", durable=True)
ch.basic_qos(prefetch_count=16)

def handle(ch, method, properties, body):
    print(body)
    ch.basic_ack(method.delivery_tag)

ch.basic_consume(queue="jobs", on_message_callback=handle)
ch.start_consuming()
```

Producer side in AMQP (publisher confirms):

```python
ch.confirm_delivery()
ch.basic_publish("", "jobs", body, properties=pika.BasicProperties(delivery_mode=2))
```

## CLI

```
ante serve   [--db PATH] [--native ADDR] [--amqp ADDR] [--amqp1 ADDR]
             [--no-native] [--no-amqp] [--security PATH]
             [--history-retention-ms N] [--history-max-rows N]
ante hash-password
ante init    [--db PATH]
ante declare QUEUE
ante list
ante info    QUEUE
ante status  QUEUE MESSAGE_ID
ante publish QUEUE [PAYLOAD] [--file PATH] [--content-type TYPE] [--message-id ID]
ante take    QUEUE [--max N] [--lease-ms N] [--wait-ms N] [--json]
ante ack     TOKEN...
ante touch   TOKEN [--lease-ms N]
ante discard TOKEN...
ante complete TOKEN QUEUE [PAYLOAD] [--file PATH] [--content-type TYPE]
             [--message-id ID] [--discard]
ante requeue TOKEN...
ante purge   QUEUE
ante delete  QUEUE
ante ping
```

`--server ADDR` (or `ANTE_SERVER`) selects the native endpoint; `--db` (or
`ANTE_DB`) selects the file used by `serve`/`init`. Default DB is
`./ante.sqlite3`. Attempt history defaults to 7 days and 1,000,000 finished
rows; pass `0` to either `serve` flag to disable that limit.

Client TLS/authentication flags: `--tls-ca PATH`, `--tls-server-name NAME`,
`--user NAME`, `--password-file PATH`; environment equivalents are `ANTE_USER`
and `ANTE_PASSWORD`. mTLS is deferred and tracked in
[docs/backlog.md](docs/backlog.md). See [docs/security.md](docs/security.md).

`take` prints one line per delivery, tab separated:

```
token  message_id  content_type  correlation_id  reply_to  attempts  payload
```

`--json` prints an array of objects instead. `--wait-ms` long-polls for a
delivery; `--lease-ms` overrides the 30 s default lease.

## Semantics

- Queues are explicit and durable. `declare` is idempotent.
- Messages are opaque bytes with optional `content_type` and `message_id`
  (native) or the standard AMQP basic properties.
- `take` moves messages to an in-flight set with a lease (default 30 s).
- `ack` deletes a message. `requeue` returns it to the ready set.
- An expired lease, or a process restart, returns in-flight messages to ready.
  Duplicate delivery is possible; that is the at-least-once contract.
- A stale token (same message, older delivery attempt) cannot ack a newer
  delivery. It returns `STALE_DELIVERY`.
- Ordering is FIFO by publish sequence within a queue. Requeued messages go to
  the back of the ready set.

See `docs/delivery-contract.md` for the full contract and failure table, and
`docs/storage-sqlite.md` for SQLite runtime and backup notes.

## Limits

| Limit | Value |
|---|---|
| Max payload | 8 MiB |
| Max batch messages | 256 |
| Max batch bytes | 1 MiB |
| Default lease | 30 s |
| AMQP frame max | 131072 |
| AMQP channel max | 16 |

## Benchmarks

Measured on a 13th Gen i7-13700H, 15.7 GiB RAM, ext4/NVMe, Rust release build.
Full method, tables, and raw data: `bench/RESULTS.md` and `bench/results/`.

The storage benchmark below predates the SQLite cutover and is retained as
historical evidence for the redb-vs-LMDB decision. The end-to-end harness
(`bench/e2e.sh`) runs the real `ante-core` engine and the native/AMQP servers
over loopback TCP; rerun it before quoting storage performance.

Real endpoints, 3 reps, median, one queue, durable commits, bounded backlog
(server and client in one process; not a network or TLS test):

| Protocol | Workload | Throughput |
|---|---|---:|
| native | 64 B, batch 256 | ~65,000–85,000 jobs/s |
| native | 1 KiB, batch 256 | ~31,000–56,000 jobs/s (high variance) |
| native | 1 KiB, batch 128 | ~37,000–42,000 jobs/s |
| native | 1 KiB, batch 1 | ~1,200–1,400 jobs/s |
| AMQP consume | 64 B/1 KiB, prefetch 256, ack multiple | ~29,000–56,000 jobs/s |
| AMQP publish | 1 KiB, confirm per message | ~1,600–2,200 jobs/s |
| AMQP publish | 1 KiB, 128 outstanding confirms | ~1,600–2,200 jobs/s |
| AMQP both | 1 KiB, batch 128 | ~1,800–3,200 jobs/s |

Storage-only prototype for reference: ~88k jobs/s at 1 KiB batch 128, 2.11 GB
and 66 MB peak RSS for a 1M × 1 KiB backlog, 0 invariant failures in 30
`kill -9` cycles.

Reading:

- Batching is mandatory. Per-message durable commits cap native at ~1.4k/s.
- The native protocol batches publishes, takes, and acks, so it reaches the
  engine's throughput. Buffering the wire writers alone was a 4x improvement.
- **AMQP publish is commit-bound.** `basic.publish` has no batch form, so every
  message is a separate durable commit and publisher confirms cannot group
  them. AMQP consumption is competitive (35–56k/s), but AMQP production tops
  out at ~2–3k messages/s until publish group commit is implemented. Use the
  native `PUBLISH_BATCH` for high-rate producers.
- Numbers are not independent fsyncs per second: at batch 256, each commit
  covers 256 messages.

## Embedding in an application

Ante is a single binary. Start it with `--native 127.0.0.1:0`, read the
printed address from stdout, and talk to it over the native protocol. Runnable
sidecar examples live in `examples/embed/` for TypeScript, Python, Go, and
Java; the same directory compares sidecar embedding with in-process FFI.

## Repository layout

```
crates/ante-core      storage engine, types, recovery
crates/ante-native    RESP2-framed native protocol (server + client)
crates/ante-amqp      AMQP 0-9-1 compatibility server
crates/ante-amqp1     AMQP 1.0 endpoint
crates/ante           daemon and CLI
docs/backlog.md       deferred work (mTLS, advanced AMQP 1.0)
examples/embed/       sidecar embedding examples (TypeScript, Python, Go, Java)
sdks/python/          Python SDK (spawn + consume/publish, synchronous)
bench/                historical storage benchmark harness and results
docs/                 protocol, delivery contract, compatibility, ADRs
```

Development:

```bash
export PATH=$HOME/.cargo/bin:$PATH
cargo test --release --workspace   # Rust unit + integration tests
cargo run -p ante -- serve --db /tmp/ante.sqlite3
```

The suite includes multi-worker stress (8 workers, no duplicate delivery, no
loss), lease expiry and stale-ack behavior over the wire, AMQP 0-9-1
compatibility tests driven by `lapin`, AMQP 1.0 tests driven by `fe2o3-amqp`,
and the delivery-contract cases in `ante-core`.

## Non-goals (v1)

- Clustering, replication, or HA.
- Named exchanges, topics, or routing beyond the AMQP default exchange.
- Transactions, scheduling/delay queues, priorities, or dead-letter exchanges.
- Multi-tenant isolation, OAuth, or audit logging.
- Exactly-once delivery.

## License

MIT
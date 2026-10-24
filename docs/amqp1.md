# AMQP 1.0 endpoint

Ante exposes a small AMQP 1.0 endpoint for clients that already speak the
standard protocol. It is separate from the AMQP 0-9-1 endpoint and starts only
when `--amqp1` is supplied:

```bash
ante serve --db ./ante.sqlite3 --amqp1 127.0.0.1:5673
# secure
ante serve --db ./ante.sqlite3 --amqp1 127.0.0.1:5673 --security ./security.json
```

The address is explicit because the rest of the daemon is loopback-first. A
single queue maps to an AMQP node address: a sender attaches a target with the
queue name, and a receiver attaches a source with the same name. Queues must be
declared before a link attaches.

The plaintext profile does not offer a SASL layer. Some clients enable SASL by
default; when connecting without `--security`, configure the client to skip
SASL (for example Apache Qpid Proton's `sasl_enabled=False`). Secure clients
use SASL as usual.

## Supported subset

- AMQP 1.0 framing and TLS protocol header (`AMQP\x02\x01\x00\x00`).
- In secure mode, SASL `PLAIN`. The plaintext development profile has no SASL
  layer. mTLS/`EXTERNAL` is deferred (see [backlog.md](backlog.md)).
- One or more sessions per connection; sender and receiver links per session.
- `Attach`, `Flow`/credit, `Transfer`, `Disposition`, `Detach`, `End`, and
  `Close` delivered by the protocol engine.
- Settled and unsettled deliveries. Accepted settles the input, released and
  modified requeue it, rejected discards it.
- Message properties: `message-id`, `correlation-id`, `reply-to`, and
  `content-type`. The body is a `data` section; `amqp-value` strings and null
  bodies are accepted on input. Binary `message-id` and `correlation-id` values
  are rejected rather than converted lossily.
- A receiver that grants zero credit does not cause a queue message to be
  claimed. The sender waits for credit before taking work. If credit arrives but
  the enqueue and local transport write cannot finish within one second, the
  connection is aborted before the claim is requeued.
- A transfer that is still buffered when its claim expires or the sender stops
  is cancelled by shutting down the connection socket and aborting the protocol
  connection. An abort descriptor is required when accepting the connection;
  failed socket shutdown retains the lease instead of immediately requeueing.
  Zero linger is requested before shutdown. All links on that connection
  reconnect and unresolved deliveries are requeued after socket shutdown.
- Transfer completion uses a local FIFO marker after the delivery's final
  fragment, carried through the session buffer to the connection writer. The
  marker completes only after the transport flush succeeds. Neither an enqueue
  nor a snapshot of session credit proves completion; exhausting the final
  window slot after a complete transfer is valid and does not force a reconnect.
- Enqueue and transport completion share one absolute `credit_wait` deadline
  (one second by default), capped by the delivery's absolute lease deadline.
  Settlement waits only until that same lease deadline. A real terminal outcome
  already available at cancellation is processed; pre-settled completion is
  accepted only after the writer completion marker succeeds.

A successful local flush is not proof of peer receipt. Bytes written before
an abort may already have reached the peer, so unresolved deliveries remain
at-least-once. A delivery whose `Accepted` disposition was already persisted
is not requeued.

The endpoint uses the same durable queue engine and the same default 30-minute
lease as AMQP 0-9-1. A receiver that never settles a delivery leaves it
in-flight until the lease expires, after which recovery makes it ready again.
Closing a receiver while the queue is empty stops its sender task instead of
leaving it polling. TLS and SASL/Open each have a 10-second deadline, and an
idle peer is disconnected.

## Message mapping

| AMQP 1.0 | Ante |
|---|---|
| target/source address | queue name |
| `data` section | payload bytes |
| `properties.message-id` | application message id |
| `properties.correlation-id` | correlation id |
| `properties.reply-to` | reply-to |
| `properties.content-type` | content type |

When Ante sends work to a receiver, the `message-id` is the original
application message id when present; otherwise it is Ante's numeric message id
as an AMQP `ulong`. Payloads are sent as a `data` section.

## Security

With `--security`, AMQP 1.0 requires TLS and SASL `PLAIN`. The security file is
the same one used by native and AMQP 0-9-1; see [security.md](security.md).
mTLS/`EXTERNAL` is deferred and tracked in [backlog.md](backlog.md).

- `PLAIN` verifies the password hash and then applies that user's queue ACLs.
- Publish requires `publish` on the target queue; receive requires `consume` on
  the source queue. Missing permission detaches the link with
  `amqp:unauthorized-access`.
- TLS accepts both a direct TLS ClientHello and the AMQP TLS upgrade header.

## What is not supported

- Transactions (`tx.*`), link resumption, and session resumption.
- Named exchanges, topics, filters, dynamic nodes, and server-named queues.
- Dead-lettering, priorities, TTLs, and delayed delivery.
- AMQP 1.0 management, CBS, or transaction coordinator links.
- Arbitrary AMQP body types beyond `data`, string `amqp-value`, and null.
- Binary `message-id` / `correlation-id` values.

Unsupported link features are rejected or detached; they are not silently
emulated.

## Dependency packaging

The patched `fe2o3-amqp` is a path dependency under
`crates/ante-amqp1/vendor/fe2o3-amqp`. Downstream applications depending on
`ante-amqp1` by path or from a checkout do not need a `[patch.crates-io]` entry;
`qa/downstream_build.sh` builds a minimal external crate in debug and release to
verify this.

## Load test

The 2026-10-24 regression run published and consumed 20,000 unique 1 KiB jobs
three times through four producer and four consumer connections, with zero
duplicates and a drained queue. See
[../qa/reports/2026-10-24-amqp1-load.md](../qa/reports/2026-10-24-amqp1-load.md).
The result is a local loopback check, not a release capacity claim.

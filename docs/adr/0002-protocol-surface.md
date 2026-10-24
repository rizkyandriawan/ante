# ADR 0002: native protocol plus an AMQP 0-9-1 subset

Date: 2026-10-23
Status: accepted; amended by ADR 0004 for AMQP heartbeats

## Context

Ante must be usable by two audiences:

1. New services that want the simplest possible client and do not want to model
   AMQP channels, exchanges, and frames.
2. Existing workers that already use an AMQP client library (pika, amqplib,
   amqp091-go) and should not need a new dependency or a custom transport.

The queue itself is small: durable named queues, publish, take/consume, ack,
requeue, purge, delete. There are no exchanges, topics, priorities, or
transactions in v1.

A single protocol would force a compromise: AMQP alone makes the simple case
carry channel/frame machinery; a custom protocol alone makes every existing
worker rewrite its transport.

## Decision

Provide **two endpoints over the same engine**:

- **Native protocol v1**: RESP2 framing with Ante-specific commands. RESP is
  used only for binary-safe arrays/bulk strings; the command set is not Redis.
- **AMQP 0-9-1 subset**: default exchange only, durable queues, consume/get,
  ack/reject/nack, publisher confirms, mandatory returns.

Both map to the same delivery contract and the same tokens/leases. Neither
endpoint has semantics the other lacks, except native `wait-ms` long polling
and AMQP's standard client ecosystem.

The native protocol is the reference surface: it is what the CLI uses, what the
integration tests exercise at the wire level, and what new clients should
implement. AMQP is a compatibility adapter, not the product identity.

## Why RESP2 and not a custom binary format

- RESP is trivial to implement and inspect (`printf ... | nc`), which keeps
  debugging cheap.
- It is binary-safe: bulk strings carry arbitrary payloads without escaping.
- Existing parsers exist in every language if someone wants one, even though
  they must implement Ante's command names.
- A custom binary format would save a few bytes per request and buy nothing
  else at this scale.

The risk is confusion with Redis. Mitigation: the protocol is documented as
"RESP2 framing, Ante commands", `HELLO` returns `ANTE 1`, and there is no
attempt to accept Redis commands (`SET`, `LPUSH`, ...). A Redis client will
connect and then fail on `HELLO` semantics rather than silently misbehave.

## AMQP scope

Included because worker clients need it:

- connection/channel lifecycle, `queue.declare`, `basic.consume`, `basic.get`,
  `basic.ack/reject/nack`, `basic.qos`, publisher confirms, mandatory returns,
  `queue.purge/delete`, connection heartbeats.

Excluded on purpose:

- named exchanges, exchange bindings, `tx.*`, TLS termination, SASL beyond
  PLAIN/AMQPLAIN, server-named queues, priority/TTL/dead-letter arguments.

Excluded features fail loudly (`540 NOT_IMPLEMENTED` channel error) or are
documented no-ops. Silently pretending to support exchanges or transactions
would create data-loss surprises.

## Consequences

Positive:

- New services get a minimal client surface; existing workers get their
  library.
- One engine, one delivery contract, one set of tests for semantics; protocol
  tests are separate.
- The AMQP adapter is replaceable: if a feature is needed, it is implemented in
  the adapter, not in the engine, unless it changes the delivery contract.

Negative:

- Two wire formats to document, test, and keep semantically aligned.
- The AMQP subset will disappoint clients that assume exchanges or
  transactions. This must be stated up front in the README and compatibility
  doc.
- The native protocol is another API to version and support.

## Alternatives rejected

- **AMQP only**: simplest engine story, but every non-AMQP service pays for
  channels and frames, and the CLI would need an AMQP client.
- **Custom protocol only**: clean, but excludes the existing worker ecosystem
  that motivated AMQP compatibility.
- **Redis protocol compatibility**: would attract Redis clients expecting Redis
  semantics (blocking pops, lists, persistence models) and create an unbounded
  compatibility obligation. RESP framing without Redis commands is the smaller
  commitment.
- **gRPC/HTTP JSON**: heavier runtime and schema tooling; poor fit for a
  single-binary daemon and binary payloads, and it still would not serve
  existing AMQP workers.
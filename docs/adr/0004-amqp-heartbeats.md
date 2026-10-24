# ADR 0004: AMQP connection heartbeats

Date: 2026-10-24
Status: accepted

## Context

AMQP consumers may run for hours. Without traffic, intermediaries and clients
cannot distinguish an idle connection from a dead one. Lease expiry is not a
substitute: it is per delivery, while a connection can be dead with no
in-flight messages.

## Decision

- Advertise a 30-second heartbeat in `connection.tune`.
- Negotiate with the AMQP rule: use the smaller non-zero value; if either side
  advertises zero, use the other side's value.
- Send a heartbeat every half interval.
- Close a peer that has been silent for two intervals with connection error
  320 `CONNECTION_FORCED`.
- Read frames in a dedicated task. The main connection loop owns the heartbeat
  timer, so a timeout cannot cancel `read_frame` in the middle of a content
  frame and lose bytes.
- On any connection exit, channel cleanup requeues outstanding deliveries
  immediately.

## Consequences

Idle clients stay connected, dead clients are detected in bounded time, and the
delivery lease remains independent of connection liveness. A client that sets
heartbeat `0` still receives the server's 30-second heartbeat, as required by
the AMQP negotiation rule.

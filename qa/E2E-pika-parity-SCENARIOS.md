# Pika / embedded contract map

## Product definition

Actors: existing Pika producers and workers, embedded Python application owners,
and AgentQueue's ingress/agent worker. They publish durable jobs, consume them,
settle successful/failed work, and restart without losing pending work.

The goal is reusable producer/handler code across the supported work-queue
profile: only connection construction changes. Ante-specific high-level JSON
encoding, atomic completion, worker concurrency, and retry policy remain explicit.

Rules: a routed publish confirm follows durable commit; manual ACK is a one-way
AMQP command; unacked work survives process failure; delivery tags belong to one
channel/attempt; reject is not successful ACK; unsupported broker policies must
be rejected. This is a supported subset, not the complete RabbitMQ feature set.

## Clusters and scenarios

| Cluster | Concrete scenario / assertion | Severity |
|---|---|---|
| API shape drift | Compare names, kinds, and defaults of parameters for 15 channel methods against Pika 1.3.2 and 1.4.4; only optional keyword-only extensions | P2 |
| False durability/routing success | Routed confirmed publish persists; missing mandatory publish raises unroutable; non-mandatory missing publish drops; queue lookup storage error must not confirm success | P1 |
| Payload/property corruption | Binary `00 ff`, empty body, metadata identity/correlation/reply fields, absent vs empty properties survive direct and wire adapters | P1 |
| Wrong settlement/ownership | NACK requeues with redelivery flag; reject records discarded; valid multiple ACK settles prefix; unknown upper tag settles nothing; close requeues unacked; recover returns claim | P1 |
| Wrong admission under contention | Two consumers with per-consumer QoS=1 each receive work before ACK; global QoS=1 allows only one; QoS=0 delivers 40 unacked messages; callback return alone does not release credit | P2 |
| Lost failure handoff | Callback error propagates without implicit retry/drop; injected manual/auto-ACK/discard storage failures close AMQP channel and preserve pending work; unsupported lifecycle/TTL options fail explicitly | P1 |
| Process/topology drift | Independent Pika producer/consumer processes move 100 ordered messages through standalone and embedded listeners; embedded abrupt exit recovers claimed work; SIGKILL matrix covers publish/claim/ACK/complete/lock | P1 |

## Execution layers

- `ante-python/tests/test_pika_contract.py`: shared channel calls against direct
  embedded, Pika to embedded listener, Pika to standalone, and Pika to RabbitMQ.
  Additional tests inject real SQLite failures and run separate Python processes.
- Existing `ante-python/tests/test_channel.py`: worker concurrency, cancellation,
  manual credit after callback return, queue identity, and lease staleness.
- `crates/ante-amqp/tests/compat.rs`: Lapin protocol/lifecycle regressions,
  including explicit shared/global prefetch.
- `qa/fault_matrix.py`: 15 SIGKILL cases using the rebuilt embedded engine.
- AgentQueue full unittest suite: AMQP ingress, actual embedded queue integration,
  CLI/security/lifecycle; external agent execution is mocked by its test fixtures.

Optional standalone/reference endpoints must be supplied explicitly. Omission
means those variants were not exercised, not that they passed. See the run report
for actual endpoints/versions and quantified results.

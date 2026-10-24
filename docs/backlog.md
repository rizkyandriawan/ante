# Backlog

Deferred work is recorded here with enough context to resume it without
reconstructing the design from chat history.

## AMQP 1.0 review follow-ups

**Status:** open after the ordered transport-completion changes (2026-10-24).
The last verification passed 131 Rust tests and 33 Python tests; these findings
are not covered by that result.

1. **Isolate known pre-transfer errors to their link.** `run_sender` currently
   aborts the whole connection for every `SendError`, including the local
   `MessageSizeExceeded` check that runs before any transfer is enqueued.
   Distinguish definitely-unsent failures from ambiguous transfers. Verify with
   two links on one connection: an oversized message on A must return to ready
   while B can still receive and ACK without reconnecting.
2. **Move settlement storage work off Tokio workers.** `settle()` synchronously
   calls `ack`, `requeue`, and `discard`, including the writer mutex and durable
   commit. Use lifecycle-tracked blocking work. Verify that writer contention
   during ACK does not stop unrelated connections or timers, and that shutdown
   accounts for any outstanding storage operation.

Additional regression cases: session window replenishment before the transfer
deadline must permit normal completion; a transfer finishing near lease expiry
must not extend the original settlement deadline.

## mTLS and certificate identity

**Status:** deferred. mTLS was implemented and then removed from the v1 surface.
The configuration now rejects `tls.client_ca`, `tls.cert_login_from`, and
client-certificate flags rather than silently ignoring them.

**Why deferred**

- The first implementation allowed a certificate-only user to keep a
  password-login path through the dummy Argon2 hash. Correct behavior needs a
  deliberate rule: a cert-only user must always fail `PLAIN`/native `AUTH`.
- Certificate login needs an explicit identity policy. RabbitMQ-style
  `EXTERNAL` maps a CA-verified certificate to an existing user and keeps that
  user's ACLs; a certificate must not grant permissions by itself.
- Operators need to choose between EXTERNAL-only services and mixed
  EXTERNAL + PLAIN deployments. That is a security policy decision, not only a
  protocol feature.

**Reference behavior (RabbitMQ)**

- Enable `rabbitmq_auth_mechanism_ssl` and SASL `EXTERNAL`.
- A client certificate signed by the configured CA is verified during TLS.
- `ssl_cert_login_from` selects `distinguished_name` (default), `common_name`,
  or a SAN field; the result must match an existing user.
- Permissions remain per-user vhost/resource permissions.
- Password authentication and EXTERNAL may coexist, but they are separate
  mechanisms.

**Acceptance criteria before re-landing**

1. `password_hash` becomes optional only when client authentication is enabled.
2. A user without a password hash can never authenticate via `PLAIN` or native
   `AUTH`, including with the dummy hash's password.
3. Certificate identity is matched against a real user; unknown identities are
   rejected without a fallback principal.
4. Native, AMQP 0-9-1, and AMQP 1.0 have the same mapping and ACL behavior.
5. Tests cover: valid cert, unknown CN, wrong CA, no cert, cert-only user
   attempting password auth, and forbidden queue operations.
6. A security file with mTLS-only fields is accepted only when the whole
   feature is enabled; otherwise startup fails loudly.

## AMQP 1.0 advanced features

**Status:** not implemented; the endpoint is a documented small subset.

- Transactions (`tx.*`) and transaction coordinator links.
- Link and session resumption.
- Source/target filters, selectors, and dynamic nodes.
- Arbitrary AMQP body types beyond `data`, string `amqp-value`, and null.
- Dead-lettering, priorities, TTL, and delayed delivery.
- Management/CBS endpoints.

These should not be mixed with the delivery-correctness fixes. Each needs its
own design note and interoperability tests.

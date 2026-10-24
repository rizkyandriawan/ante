# Native protocol, version 1

The native protocol is a small, binary-safe request/response protocol. It uses
RESP2 framing (the wire format Redis uses) because it is trivial to implement
and inspect with `nc`, but the command set is Ante's own. **It is not a Redis
API.**

Transport: TCP, default `127.0.0.1:7300`. With `--security` the listener uses
TLS; clients verify the server certificate against the configured CA (no client
certificate is required). See [security.md](security.md). One request in flight per connection;
requests on one connection are handled in order. A `TAKE` with `wait-ms > 0`
blocks that connection until a message arrives or the wait expires.

## Framing

A request is a RESP array of bulk strings:

```
*2\r\n$7\r\nDECLARE\r\n$4\r\njobs\r\n
```

A reply is one of:

| Byte | Type | Example |
|---|---|---|
| `+` | simple string | `+OK\r\n` |
| `-` | error | `-QUEUE_NOT_FOUND queue not found: jobs\r\n` |
| `:` | integer | `:42\r\n` |
| `$` | bulk string, `$-1` is null | `$5\r\nhello\r\n` |
| `*` | array, `*-1` is null | `*2\r\n$1\r\na\r\n$1\r\nb\r\n` |

Limits: 16 MiB per bulk value, 2048 items per request array. Requests or
replies exceeding a limit produce a protocol error and the connection is
closed.

Errors have the form `-<CODE> <message>`. Codes:

| Code | Meaning |
|---|---|
| `QUEUE_NOT_FOUND` | queue does not exist (or was deleted) |
| `MESSAGE_NOT_FOUND` | message id is unknown for that queue |
| `QUEUE_EXISTS` | queue exists where the operation requires absence |
| `PAYLOAD_TOO_LARGE` | payload exceeds 8 MiB |
| `BATCH_TOO_LARGE` | too many messages (>256) or bytes (>1 MiB) in a batch |
| `INVALID` | malformed or impossible request |
| `INTERNAL` | engine lock poisoned |
| `STORAGE` | storage-layer failure |
| `STALE_DELIVERY` | the token's delivery attempt is no longer current |
| `UNKNOWN_DELIVERY` | the token does not refer to an in-flight delivery |
| `UNKNOWN_COMMAND` | unrecognized command name |
| `PROTOCOL` | malformed request (Bad request format) |
| `VERSION` | unsupported protocol version |
| `AUTH_REQUIRED` | authenticate before using queue commands |
| `AUTHENTICATION_FAILED` | invalid username or secret |
| `PERMISSION_DENIED` | the user lacks that permission on the queue |

`STALE_DELIVERY` and `UNKNOWN_DELIVERY` come from `ACK`/`REQUEUE` results.

## Commands

Arguments are case-insensitive command names followed by positional values.

### `AUTH username secret`

Authenticates the connection when the server is started with `--security`.
Reply: `+OK`. Errors: `-AUTHENTICATION_FAILED ...`, or `-AUTH_REQUIRED ...` for
any queue command before authentication. `HELLO`, `PING`, `AUTH`, and `QUIT` are
allowed before authentication. On a legacy server without `--security`, `AUTH`
is a no-op `+OK`.

### `HELLO [version]`

Negotiates the protocol. `version` defaults to `1`; anything else returns
`-VERSION ...`. Reply: `+ANTE 1`.

### `DECLARE queue`

Creates the queue if it does not exist. Idempotent. Reply: integer queue id.

### `PUBLISH queue payload [content-type] [message-id]`

Durably appends one message. `payload` is binary. The optional fields are
UTF-8 strings; an empty string means absent. Reply: integer message id.

The queue must exist. Publishing to an unknown queue returns `QUEUE_NOT_FOUND`.

### `PUBLISH_BATCH queue payload...`

Durably appends all payloads in one commit. The batch is atomic: either every
message is stored or none is. Limits: 256 messages, 1 MiB total payload.
Reply: array of message ids. No per-message properties in a batch.

### `TAKE queue max [lease-ms] [wait-ms]`

Moves up to `max` messages from ready to in-flight and returns them. `max` is
capped at 256. `lease-ms` defaults to `30000`; `0` also means the default.
`wait-ms` long-polls: if the queue is empty, the server retries for up to that
duration before returning an empty array.

Reply: array of records. Each record is an array of seven bulk strings:

| Index | Field |
|---:|---|
| 0 | delivery token (`<message-id>:<delivery-id>`) |
| 1 | message id (integer as text) |
| 2 | content type, or empty |
| 3 | correlation id, or empty (AMQP-originated messages) |
| 4 | reply-to, or empty (AMQP-originated messages) |
| 5 | attempts, 1 for the first delivery |
| 6 | payload, binary |

An empty queue returns `*0\r\n`.

### `ACK token...` (alias `ACK_BATCH`)

Acknowledges one or more deliveries in one commit and deletes the messages.
One token returns a simple string (`+OK`) or an error. Multiple tokens return
an array with one result per token, in request order:

- `+OK` — acked.
- `-STALE_DELIVERY` — the message was redelivered; this token is obsolete.
  The newer delivery is untouched.
- `-UNKNOWN_DELIVERY` — no matching in-flight delivery (already acked, or
  lease expired and the message was requeued).

A stale or unknown result does **not** fail the other tokens in the batch.

### `REQUEUE token`

Returns the message to the back of the ready set and increments its attempt
count. Same stale/unknown rules as `ACK`.

Native `REQUEUE` exists because at-least-once workers need an explicit retry.
The AMQP endpoint maps `basic.reject`/`basic.nack` with `requeue=true` to it.

### `DISCARD token...`

Permanently removes one or more deliveries without recording them as
successful. Same token, batching, and stale/unknown rules as `ACK`; the attempt
history records `discarded` instead of `acked`. The AMQP endpoint maps
`basic.reject`/`basic.nack` with `requeue=false` to it.

### `COMPLETE token queue payload [content-type] [message-id] [ACK|DISCARD]`

Publishes one result message to `queue` and settles the token in one
transaction, then returns the new message id. `ACK` (the default) records the
input attempt as `acked`; `DISCARD` records it as `discarded`. Requires
`CONSUME` on the input queue and `PUBLISH` on the destination queue.

A stale or unknown token returns the same error as `ACK` and publishes nothing.
A missing destination queue, oversized payload, permission failure, or storage
error aborts the transaction, leaving the input delivery in flight. If the
destination is the same queue, the new result is enqueued and the input entry is
removed atomically.

### `INFO queue`

Reply: a flat array of key/value bulk strings:

```
name <name> id <n> ready <n> inflight <n> total <n>
```

`total` is `ready + inflight`. `id` is the internal queue id.

### `STATUS queue message-id`

Returns the current state and retained attempt history for one message.
Requires `INSPECT` permission. Reply: a flat array of key/value pairs:

```
queue <name> message-id <n> state <state> attempts <n>
created-at-ms <n> updated-at-ms <n> history <records>
```

`state` is one of `ready`, `inflight`, `acked`, `requeued`, `expired`,
`recovered`, or `discarded`. `discarded` also covers a message that was removed
while ready (for example by queue deletion); its history still shows the last
attempt outcome. `history` is an array of records; each record is
a flat array of key/value pairs:

```
attempt <n> delivery-id <n> started-at-ms <n>
finished-at-ms <n|empty> outcome <state>
```

History is retained after ack and after queue deletion. A message id that never
existed, or whose history belongs to another queue, returns
`-MESSAGE_NOT_FOUND ...`.

### `LIST`

Reply: an array of queues. Each queue is an array of three bulk strings:
`name`, `ready`, `inflight`. Order is unspecified.

### `PURGE queue`

Deletes all ready and in-flight messages in the queue. Reply: integer count of
messages removed.

### `DELETE queue`

Deletes the queue, its ready messages, and its in-flight messages. Reply:
integer count of messages removed. In-flight deliveries become unknown.

### `PING`

Reply: `+PONG`.

### `QUIT`

Reply: `+OK`, then the server closes the connection.

## Delivery tokens

Tokens are text: `<message-id>:<delivery-id>`. Both parts are unsigned 64-bit
decimal integers. Tokens are opaque to clients; do not parse or construct them.
A token is valid only for the in-flight delivery it was issued for. After a
lease expiry, requeue, or ack, the token is stale or unknown.

## Long polling

`TAKE q 1 30000 5000` waits up to 5 s. The server polls internally at short
intervals; there is no push notification in v1. Workers should prefer long
polling over tight loops.

## Examples

Declare and publish:

```
C: *2\r\n$7\r\nDECLARE\r\n$4\r\njobs\r\n
S: :1\r\n
C: *3\r\n$7\r\nPUBLISH\r\n$4\r\njobs\r\n$5\r\nhello\r\n
S: :1\r\n
```

Take and ack:

```
C: *4\r\n$4\r\nTAKE\r\n$4\r\njobs\r\n$1\r\n1\r\n$5\r\n30000\r\n
S: *1\r\n*7\r\n$3\r\n1:1\r\n$1\r\n1\r\n$0\r\n\r\n$0\r\n\r\n$0\r\n\r\n$1\r\n1\r\n$5\r\nhello\r\n
C: *2\r\n$3\r\nACK\r\n$3\r\n1:1\r\n
S: +OK\r\n
```

## Versioning

`HELLO 1` is the only version. Any incompatible change increments the version.
Additive commands and optional trailing arguments do not. Clients should send
`HELLO` first and fail closed on a version mismatch.
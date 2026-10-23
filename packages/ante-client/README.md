# ante-client

Typed TypeScript client for [Ante](../../README.md), built on `amqplib`.
Ante exposes an AMQP 0-9-1 subset: default exchange only, durable queues,
publisher confirms, manual ack, and prefetch.

## Install

```bash
npm install ante-client
# or, inside this repository:
npm install ./packages/ante-client
```

Requires Node 18 or newer.

## Consume

```ts
import { Ante } from "ante-client";

const ante = await Ante.connect("amqp://guest:guest@127.0.0.1:5672", {
  onError: (error) => console.error("ante error", error),
});

const consumer = await ante.consume<{ task: string; id: number }>(
  "jobs",
  async (delivery) => {
    await resize(delivery.payload);
  },
  {
    prefetch: 16,
    onError: async (error, delivery) => {
      console.error("job failed", delivery.payload.id, error);
      return delivery.redelivered ? "drop" : "requeue";
    },
  },
);

process.once("SIGINT", () => {
  void consumer.stop().then(() => ante.close());
});
```

The handler may be async. A delivery is acked after it resolves, unless the
handler already called `ack()` or `nack()`. If it throws, `onError` decides;
without `onError` the message is logged and requeued.

`consumer.cancelled` resolves when the broker cancels the subscription, for
example because the queue was deleted. Resubscribe if the queue is recreated.
`consumer.stop()` only stops locally.

## Publish

```ts
await ante.publish("jobs", { task: "resize", id: 42 });
```

Objects become JSON (`content-type: application/json`), strings become
`text/plain`, and `Buffer`/`Uint8Array` are sent as-is. Publishing waits for
the broker's publisher confirm, so a resolved promise means the message is
durable. `persistent: true` is the default.

## Queue helpers

```ts
const info = await ante.queueInfo("jobs");   // { queue, messageCount, consumerCount }
await ante.purgeQueue("jobs");
await ante.deleteQueue("jobs", { ifEmpty: true });
```

## Ante specifics

- **Routing key is the queue name.** Ante has no named exchanges; publishing
  uses the default exchange (`""`).
- **Prefetch is per channel.** Each `consume()` call creates its own channel,
  which also means one slow handler pool cannot starve another consumer.
- **Durability is the default.** A publish resolves only after a durable
  commit. Consume-batch throughput is high; AMQP publish is one commit per
  message (see the repository benchmark).
- **At-least-once.** A crash or lease expiry can redeliver a message. Handlers
  must be idempotent.
- **No auth or TLS yet.** Ante is a single-node, local-first daemon; use the
  URL form the server accepts (`amqp://guest:guest@host:port`).

## API

| Method | Description |
|---|---|
| `Ante.connect(url, options?)` | Open a connection and confirm channel |
| `publish(queue, payload, options?)` | Publish and wait for a confirm |
| `consume(queue, handler, options?)` | Declare, prefetch, and consume on a dedicated channel |
| `declareQueue(queue, options?)` | Idempotent queue declaration |
| `queueInfo(queue)` | Ready and consumer counts |
| `deleteQueue(queue, options?)` | Delete, optionally guarded by `ifUnused`/`ifEmpty` |
| `purgeQueue(queue)` | Discard ready messages |
| `close()` | Close the connection |

A `Consumer` exposes `tag`, `cancelled`, and `stop()`; `cancelled` is a promise
that resolves on a broker-initiated cancel.

Options for `consume`: `prefetch`, `noAck`, `queue`, `parse`, `onError`.
`onError` returns `"requeue"` (default) or `"drop"`. Connect options:
`prefetch` and `onError`.

## Development

```bash
npm install
npm run build
npm test
ANTE_URL=amqp://guest:guest@127.0.0.1:5673 npm run smoke
```

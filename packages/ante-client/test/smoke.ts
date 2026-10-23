import assert from "node:assert/strict";

import { Ante } from "../src/index.js";

const url = process.env.ANTE_URL ?? "amqp://guest:guest@127.0.0.1:5672";
const queue = `ante-client-smoke-${Date.now()}`;
const timeoutMs = 5_000;

const withTimeout = <T>(promise: Promise<T>, label: string): Promise<T> =>
  Promise.race([
    promise,
    new Promise<never>((_, reject) =>
      setTimeout(() => reject(new Error(`${label} timed out`)), timeoutMs),
    ),
  ]);

const errors: Error[] = [];
const ante = await Ante.connect(url, {
  onError: (error) => {
    errors.push(error);
    console.error("[smoke] client error:", error.message);
  },
});
let consumer: Awaited<ReturnType<typeof ante.consume>> | undefined;
let queueExists = true;
try {
  await ante.declareQueue(queue);

  const received = new Promise<{ task: string; id: number }>((resolve) => {
    void ante
      .consume<{ task: string; id: number }>(
        queue,
        (delivery) => {
          console.log("received", delivery.payload, "redelivered", delivery.redelivered);
          resolve(delivery.payload);
        },
        { prefetch: 1 },
      )
      .then((started) => {
        consumer = started;
      });
  });

  await ante.publish(queue, { task: "resize", id: 42 });
  const payload = await withTimeout(received, "consume");
  assert.deepEqual(payload, { task: "resize", id: 42 });

  // Let the consume-ok settle before inspecting the count.
  await new Promise((resolve) => setTimeout(resolve, 50));
  const info = await ante.queueInfo(queue);
  assert.equal(info.messageCount, 0);
  assert.equal(info.consumerCount, 1);
  console.log("queue", info);

  await ante.deleteQueue(queue);
  queueExists = false;
  await withTimeout(consumer!.cancelled, "broker cancellation");
  console.log("broker cancelled the consumer");
} finally {
  await consumer?.stop().catch(() => undefined);
  if (queueExists) {
    await ante.deleteQueue(queue).catch(() => undefined);
  }
  await ante.close();
}

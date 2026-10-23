import amqp, {
  type Channel,
  type ChannelModel,
  type ConfirmChannel,
  type ConsumeMessage,
  type Options,
  type Replies,
} from "amqplib";

import { decodePayload, encodePayload } from "./codec.js";
import type {
  ConnectOptions,
  ConsumeOptions,
  Consumer,
  Delivery,
  ErrorAction,
  Handler,
  PublishOptions,
  QueueInfo,
  QueueOptions,
} from "./types.js";

const DEFAULT_PREFETCH = 16;

export class AnteError extends Error {
  constructor(
    message: string,
    readonly cause?: unknown,
  ) {
    super(message);
    this.name = "AnteError";
  }
}

export class Ante {
  readonly #connection: ChannelModel;
  readonly #channel: ConfirmChannel;
  readonly #defaultPrefetch: number;
  readonly #onError: (error: Error) => void;
  #closed = false;

  private constructor(
    connection: ChannelModel,
    channel: ConfirmChannel,
    options: ConnectOptions,
  ) {
    this.#connection = connection;
    this.#channel = channel;
    this.#defaultPrefetch = options.prefetch ?? DEFAULT_PREFETCH;
    this.#onError =
      options.onError ??
      ((error) => {
        console.error("[ante-client]", error);
      });

    connection.on("error", (error) => {
      this.#closed = true;
      this.#onError(error);
    });
    connection.on("close", () => {
      this.#closed = true;
    });
    channel.on("error", (error) => {
      this.#closed = true;
      this.#onError(error);
    });
    channel.on("close", () => {
      this.#closed = true;
    });
  }

  static async connect(url: string, options: ConnectOptions = {}): Promise<Ante> {
    const connection = await amqp.connect(url);
    try {
      const channel = await connection.createConfirmChannel();
      return new Ante(connection, channel, options);
    } catch (error) {
      await connection.close().catch(() => undefined);
      throw new AnteError("could not open the client channel", error);
    }
  }

  /** Publish one message to a queue and wait for its publisher confirm. */
  async publish<T>(
    queue: string,
    payload: T,
    options: PublishOptions = {},
  ): Promise<void> {
    this.#assertOpen();
    const encoded = encodePayload(payload);
    const publishOptions: Options.Publish = {
      persistent: options.persistent ?? true,
      contentType: options.contentType ?? encoded.contentType,
      messageId: options.messageId,
      correlationId: options.correlationId,
      replyTo: options.replyTo,
      headers: options.headers,
    };

    await new Promise<void>((resolve, reject) => {
      this.#channel.publish(
        "",
        queue,
        encoded.body,
        publishOptions,
        (error) => (error ? reject(error) : resolve()),
      );
    });
  }

  /**
   * Declare a queue and start consuming. Each consumer gets its own channel,
   * because Ante applies prefetch per channel.
   */
  async consume<T>(
    queue: string,
    handler: Handler<T>,
    options: ConsumeOptions<T> = {},
  ): Promise<Consumer> {
    this.#assertOpen();
    const channel = await this.#connection.createChannel();
    channel.on("error", (error) => {
      this.#onError(error);
    });
    try {
      await channel.assertQueue(queue, queueOptions(options.queue));
      await channel.prefetch(options.prefetch ?? this.#defaultPrefetch);

      let resolveCancelled!: () => void;
      const cancelled = new Promise<void>((resolve) => {
        resolveCancelled = resolve;
      });
      let tag = "";
      channel.on("cancel", (fields) => {
        // Each consumer owns its channel, so any cancel here belongs to it.
        if (!tag || fields.consumerTag === tag) {
          resolveCancelled();
        }
      });

      const reply = await channel.consume(
        queue,
        (message) => {
          if (message) {
            void handleMessage(channel, queue, message, handler, options);
          }
        },
        { noAck: options.noAck ?? false },
      );
      tag = reply.consumerTag;

      let stopped = false;
      return {
        queue,
        tag,
        cancelled,
        async stop() {
          if (stopped) return;
          stopped = true;
          await channel.cancel(tag).catch(() => undefined);
          await channel.close().catch(() => undefined);
        },
      };
    } catch (error) {
      await channel.close().catch(() => undefined);
      throw new AnteError(`could not consume from "${queue}"`, error);
    }
  }

  async declareQueue(queue: string, options: QueueOptions = {}): Promise<QueueInfo> {
    this.#assertOpen();
    return toQueueInfo(await this.#channel.assertQueue(queue, queueOptions(options)));
  }

  async queueInfo(queue: string): Promise<QueueInfo> {
    this.#assertOpen();
    return toQueueInfo(await this.#channel.checkQueue(queue));
  }

  /** Delete a queue. Returns the number of messages discarded. */
  async deleteQueue(
    queue: string,
    options: { ifUnused?: boolean; ifEmpty?: boolean } = {},
  ): Promise<number> {
    this.#assertOpen();
    const reply = await this.#channel.deleteQueue(queue, {
      ifUnused: options.ifUnused ?? false,
      ifEmpty: options.ifEmpty ?? false,
    });
    return reply.messageCount;
  }

  /** Discard ready messages. Returns the number purged. */
  async purgeQueue(queue: string): Promise<number> {
    this.#assertOpen();
    const reply = await this.#channel.purgeQueue(queue);
    return reply.messageCount;
  }

  async close(): Promise<void> {
    if (this.#closed) return;
    this.#closed = true;
    await this.#connection.close().catch(() => undefined);
  }

  #assertOpen(): void {
    if (this.#closed) {
      throw new AnteError("client is closed");
    }
  }
}

function queueOptions(options: QueueOptions = {}): Options.AssertQueue {
  return {
    durable: options.durable ?? true,
    autoDelete: options.autoDelete ?? false,
    exclusive: options.exclusive ?? false,
  };
}

function toQueueInfo(reply: Replies.AssertQueue): QueueInfo {
  return {
    queue: reply.queue,
    messageCount: reply.messageCount,
    consumerCount: reply.consumerCount,
  };
}

async function handleMessage<T>(
  channel: Channel,
  queue: string,
  message: ConsumeMessage,
  handler: Handler<T>,
  options: ConsumeOptions<T>,
): Promise<void> {
  let delivery: Delivery<T>;
  try {
    delivery = wrapDelivery<T>(channel, message, options);
  } catch (error) {
    // An undecodable message cannot be handled; requeueing it would loop.
    console.error(`[ante-client] dropping undecodable message from "${queue}":`, error);
    channel.nack(message, false, false);
    return;
  }

  try {
    await handler(delivery);
    if (!options.noAck) {
      delivery.ack();
    }
  } catch (error) {
    if (options.noAck) {
      console.error(`[ante-client] auto-acked handler failed on "${queue}":`, error);
      return;
    }

    let action: ErrorAction = "requeue";
    if (options.onError) {
      try {
        action = await options.onError(error, delivery);
      } catch (nested) {
        console.error("[ante-client] onError failed; requeueing:", nested);
        action = "requeue";
      }
    } else {
      console.error(`[ante-client] handler failed on "${queue}"; requeueing:`, error);
    }
    delivery.nack(action === "requeue");
  }
}

function wrapDelivery<T>(
  channel: Channel,
  message: ConsumeMessage,
  options: ConsumeOptions<T>,
): Delivery<T> {
  let settled = false;
  const payload = decodePayload<T>(
    message.content,
    message.properties.contentType,
    options.parse,
  );

  return {
    payload,
    raw: message.content,
    redelivered: message.fields.redelivered,
    messageId: message.properties.messageId,
    correlationId: message.properties.correlationId,
    replyTo: message.properties.replyTo,
    contentType: message.properties.contentType,
    ack() {
      if (settled) return;
      settled = true;
      channel.ack(message);
    },
    nack(requeue = true) {
      if (settled) return;
      settled = true;
      channel.nack(message, false, requeue);
    },
  };
}

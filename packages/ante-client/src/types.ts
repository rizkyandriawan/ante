export type ErrorAction = "requeue" | "drop";

export interface Delivery<T = unknown> {
  /** Decoded payload. JSON content types are parsed automatically. */
  readonly payload: T;
  /** Raw message body. */
  readonly raw: Buffer;
  readonly redelivered: boolean;
  readonly messageId?: string;
  readonly correlationId?: string;
  readonly replyTo?: string;
  readonly contentType?: string;
  /** Acknowledge the delivery. Safe to call more than once. */
  ack(): void;
  /** Reject the delivery. `requeue` defaults to true. Safe to call more than once. */
  nack(requeue?: boolean): void;
}

export type Handler<T> = (delivery: Delivery<T>) => void | Promise<void>;

export interface QueueOptions {
  durable?: boolean;
  autoDelete?: boolean;
  exclusive?: boolean;
}

export interface ConsumeOptions<T> {
  /** Unacked deliveries allowed on this consumer's channel. Default 16. */
  prefetch?: number;
  /** Let the broker auto-ack. The handler cannot retry failed jobs. */
  noAck?: boolean;
  /** Queue declaration used before consuming. Durable by default. */
  queue?: QueueOptions;
  /** Custom decoder. Throw to reject the message permanently. */
  parse?: (raw: Buffer, contentType?: string) => T;
  /**
   * Decide what to do when the handler throws. Default: log and requeue.
   * Return `"drop"` to discard, or `"requeue"` to retry.
   */
  onError?: (error: unknown, delivery: Delivery<T>) => ErrorAction | Promise<ErrorAction>;
}

export interface Consumer {
  readonly queue: string;
  readonly tag: string;
  /**
   * Resolves when the broker cancels this consumer, for example because its
   * queue was deleted. Resubscribe if the queue is recreated.
   */
  readonly cancelled: Promise<void>;
  /** Stop new deliveries and close the consumer channel. */
  stop(): Promise<void>;
}

export interface PublishOptions {
  persistent?: boolean;
  contentType?: string;
  messageId?: string;
  correlationId?: string;
  replyTo?: string;
  headers?: Record<string, unknown>;
}

export interface QueueInfo {
  queue: string;
  messageCount: number;
  consumerCount: number;
}

export interface ConnectOptions {
  /** Prefetch used by `consume` when the call does not set one. Default 16. */
  prefetch?: number;
  /**
   * Receives connection and channel errors. The default logs to stderr.
   * Without a listener, an AMQP channel error would be an unhandled event.
   */
  onError?: (error: Error) => void;
}

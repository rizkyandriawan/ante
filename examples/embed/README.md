# Embedding Ante in an application

Ante is one Rust binary and one database file. There are two ways to embed it:

1. **Sidecar (used by these examples).** Ship the binary with your app, start
   it as a child process, and talk to the native RESP2 endpoint over a local
   TCP socket. Language-agnostic, crash-isolated, and no FFI or per-platform
   native module.
2. **In-process FFI.** Compile `ante-core` into a native library and call it
   through Node-API (napi-rs), PyO3, cgo, or JNI. No child process and no
   socket, but every target needs a prebuilt artifact, and the Rust async
   runtime must be bridged into the host runtime.

For Ante's deployment model ("an application embeds or ships one daemon"),
start with the sidecar. Move to FFI only if the process boundary or the socket
shows up in a profile.

## Sidecar lifecycle (same in every language)

1. Spawn `ante serve --db <path> --no-amqp --native 127.0.0.1:0`.
2. Read the child's stdout until the line `native  127.0.0.1:<port>`. Port `0`
   lets the OS pick a free port; the server prints the real one.
3. Connect to that address and send `HELLO 1`; expect `+ANTE 1`.
4. Use `DECLARE`, `PUBLISH`, `PUBLISH_BATCH`, `TAKE`, `ACK`, `REQUEUE`,
   `INFO`, `PURGE`, `DELETE`.
5. On shutdown, send `SIGTERM`, wait up to ~6 s, then kill. Close sockets
   first if you want to interrupt a long-polling `TAKE`.

Only one process may own a given `.redb` file. Give each app instance its own
database path, or share one daemon over the network.

## Examples

| Language | Run |
|---|---|
| TypeScript / Node | `ANTE_BIN=./target/release/ante node examples/embed/typescript/example.mjs` |
| Python 3 | `ANTE_BIN=./target/release/ante python3 examples/embed/python/example.py` |
| Go | `cd examples/embed/go && ANTE_BIN=../../target/release/ante go run .` |
| Java 21+ | `ANTE_BIN=./target/release/ante java examples/embed/java/Example.java` |

Each example starts a daemon, declares `demo`, publishes a JSON message,
takes it, acks it, prints `INFO`, and shuts the daemon down.

## Packaging the binary

- **TypeScript / Node:** publish per-platform packages and select one with
  `optionalDependencies` (the esbuild pattern), or resolve `ANTE_BIN` /
  a path shipped in your app bundle. Spawn it; never block the event loop.
- **Python:** put the binary in the wheel as package data and locate it with
  `importlib.resources`, or download the right artifact on first run. Wheels
  are already platform-specific, so one binary per wheel is natural.
- **Go:** `//go:embed` the binary, write it to a cache directory (mode 0755)
  on first start, and `exec` that path. This gives a single Go binary that
  carries Ante.
- **Java:** put the binary in the JAR under `/native/<os>-<arch>/ante`, extract
  it to a temp directory at startup, `chmod +x`, and use `ProcessBuilder`.
  Delete it on shutdown (or reuse a stable cache path).

## In-process FFI, if you need it

- **TypeScript:** [napi-rs](https://napi.rs) wrapping a small Rust crate that
  owns `ante_core::Ante` and exposes `declare/publish/take/ack` as async
  functions. Build `.node` per platform.
- **Python:** [PyO3](https://pyo3.rs) + maturin; return `await`-able futures or
  run blocking engine calls on a dedicated thread.
- **Go:** build `ante-core` as a `cdylib`/`staticlib` and call it with cgo, or
  generate bindings with `cbindgen`. Keep the tokio runtime inside Rust.
- **Java:** JNI (or the FFM API in Java 22+) against a `cdylib`. Marshal tokens
  and payloads as byte arrays; do not block the JVM thread on the runtime for
  long.

The sidecar avoids all of that and keeps the daemon's crash domain separate
from the app. The native protocol is one request in flight per connection, so
a worker that long-polls `TAKE` should use its own connection; acknowledgements
and publishes can use another.

## Function signatures to copy

The examples use low-level commands. In application code, wrap them with a
consumer shape inspired by RabbitMQ clients:

```ts
type Handler<T> = (payload: T, delivery: Delivery) => void | Promise<void>;

consume<T>(queue: string, handler: Handler<T>, options?: {
  prefetch?: number;
  leaseMs?: number;
  waitMs?: number;
  onError?: (error: unknown, delivery: Delivery) => "requeue" | "drop";
}): Promise<Consumer>;
```

`Delivery` carries `token`, `messageId`, `attempts`, `contentType`, and the raw
payload. Ack after the handler resolves; `REQUEUE` on a retryable failure; ack
(drop) on a permanent failure. Use `attempts` for a retry limit, and remember
that a handler running longer than `leaseMs` can be redelivered.
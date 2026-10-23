from __future__ import annotations

import os
import select
import subprocess
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable, Iterable

from .codec import Parser, decode_payload, encode_payload
from .protocol import AnteError, Connection, RespError


@dataclass
class AckResult:
    ok: bool
    code: str | None = None
    message: str = ""


@dataclass
class Delivery:
    token: str
    message_id: int
    attempts: int
    raw: bytes
    content_type: str | None
    correlation_id: str | None
    reply_to: str | None
    payload: Any
    _connection: Connection = field(repr=False)
    _settled: bool = field(default=False, repr=False)

    @property
    def settled(self) -> bool:
        return self._settled

    def ack(self) -> None:
        if self._settled:
            return
        self._connection.request("ACK", self.token)
        self._settled = True

    def requeue(self) -> None:
        if self._settled:
            return
        self._connection.request("REQUEUE", self.token)
        self._settled = True


Handler = Callable[[Any, Delivery], None]
ErrorPolicy = Callable[[BaseException, Delivery], str]


class Ante:
    def __init__(
        self,
        control: Connection,
        *,
        host: str,
        port: int,
        process: subprocess.Popen[str] | None = None,
        startup_timeout: float = 10.0,
    ) -> None:
        self._control = control
        self._host = host
        self._port = port
        self._process = process
        self._startup_timeout = startup_timeout
        self._consumers: set[Any] = set()
        self._closed = False

    @classmethod
    def connect(
        cls,
        host: str = "127.0.0.1",
        port: int = 7300,
        *,
        timeout: float = 10.0,
    ) -> Ante:
        return cls(Connection.open(host, port, timeout), host=host, port=port)

    @classmethod
    def open(
        cls,
        db: str | os.PathLike[str],
        *,
        binary: str | None = None,
        timeout: float = 10.0,
        startup_timeout: float = 10.0,
        extra_args: Iterable[str] = (),
    ) -> Ante:
        path = Path(db)
        path.parent.mkdir(parents=True, exist_ok=True)
        executable = binary or os.environ.get("ANTE_BIN") or "ante"
        args = [
            executable,
            "serve",
            "--db",
            str(path),
            "--no-amqp",
            "--native",
            "127.0.0.1:0",
            *extra_args,
        ]
        process = subprocess.Popen(
            args,
            stdout=subprocess.PIPE,
            stderr=None,
            text=True,
            bufsize=1,
        )
        try:
            host, port = _read_address(process, startup_timeout)
            control = Connection.open(host, port, timeout)
        except Exception:
            _stop_process(process, 2.0)
            raise
        return cls(
            control,
            host=host,
            port=port,
            process=process,
            startup_timeout=startup_timeout,
        )

    def __enter__(self) -> Ante:
        return self

    def __exit__(self, *_exc: object) -> None:
        self.close()

    @property
    def host(self) -> str:
        return self._host

    @property
    def port(self) -> int:
        return self._port

    def declare(self, queue: str) -> int:
        return int(self._control.request("DECLARE", queue))

    def publish(
        self,
        queue: str,
        payload: Any,
        *,
        content_type: str | None = None,
        message_id: str | None = None,
    ) -> int:
        body, default_type = encode_payload(payload)
        return int(
            self._control.request(
                "PUBLISH",
                queue,
                body,
                content_type or default_type,
                message_id or "",
            )
        )

    def publish_batch(self, queue: str, payloads: Iterable[Any]) -> list[int]:
        bodies: list[bytes] = []
        for payload in payloads:
            body, content_type = encode_payload(payload)
            if content_type == "application/octet-stream":
                raise ValueError(
                    "publish_batch encodes JSON and text only; use publish() for bytes"
                )
            bodies.append(body)
        reply = self._control.request("PUBLISH_BATCH", queue, *bodies)
        return [int(value) for value in reply]

    def take(
        self,
        queue: str,
        *,
        max: int = 1,
        lease_ms: int = 30_000,
        wait_ms: int = 0,
        parse: Parser | None = None,
    ) -> list[Delivery]:
        return self._take_on(self._control, queue, max, lease_ms, wait_ms, parse)

    def _take_on(
        self,
        connection: Connection,
        queue: str,
        max: int,
        lease_ms: int,
        wait_ms: int,
        parse: Parser | None = None,
        settle_connection: Connection | None = None,
    ) -> list[Delivery]:
        args: list[Any] = ["TAKE", queue, max, lease_ms]
        if wait_ms:
            args.append(wait_ms)
        records = connection.request(*args)
        settle = settle_connection or connection
        return [self._delivery(record, parse, settle) for record in records]

    def _delivery(
        self,
        record: list[Any],
        parse: Parser | None,
        connection: Connection,
    ) -> Delivery:
        token, message_id, content_type, correlation_id, reply_to, attempts, body = record
        raw = bytes(body) if isinstance(body, (bytes, bytearray)) else bytes(str(body), "utf-8")
        kind = _text(content_type) or None
        return Delivery(
            token=_text(token),
            message_id=int(_text(message_id)),
            attempts=int(_text(attempts)),
            raw=raw,
            content_type=kind,
            correlation_id=_text(correlation_id) or None,
            reply_to=_text(reply_to) or None,
            payload=decode_payload(raw, kind, parse),
            _connection=connection,
        )

    def ack(self, *tokens: str) -> list[AckResult]:
        if not tokens:
            return []
        reply = self._control.request("ACK", *tokens)
        if len(tokens) == 1:
            return [AckResult(True)]
        results: list[AckResult] = []
        for value in reply:
            if isinstance(value, RespError):
                results.append(AckResult(False, value.code, value.message))
            else:
                results.append(AckResult(True))
        return results

    def requeue(self, token: str) -> None:
        self._control.request("REQUEUE", token)

    def info(self, queue: str) -> dict[str, Any]:
        values = [_text(value) for value in self._control.request("INFO", queue)]
        data = dict(zip(values[0::2], values[1::2]))
        return {
            "name": data.get("name", queue),
            "id": int(data.get("id", 0)),
            "ready": int(data.get("ready", 0)),
            "inflight": int(data.get("inflight", 0)),
            "total": int(data.get("total", 0)),
        }

    def list_queues(self) -> list[dict[str, Any]]:
        reply = self._control.request("LIST")
        queues = []
        for name, ready, inflight in reply:
            queues.append(
                {
                    "name": _text(name),
                    "ready": int(_text(ready)),
                    "inflight": int(_text(inflight)),
                }
            )
        return queues

    def purge(self, queue: str) -> int:
        return int(self._control.request("PURGE", queue))

    def delete_queue(self, queue: str) -> int:
        return int(self._control.request("DELETE", queue))

    def ping(self) -> bool:
        return self._control.request("PING") == "PONG"

    def consume(
        self,
        queue: str,
        handler: Handler,
        *,
        prefetch: int = 16,
        lease_ms: int = 30_000,
        wait_ms: int = 5_000,
        parse: Parser | None = None,
        on_error: ErrorPolicy | None = None,
        max_attempts: int | None = 5,
        declare: bool = True,
        shutdown_timeout: float = 10.0,
    ) -> Any:
        from .consumer import Consumer

        if self._closed:
            raise AnteError("CLOSED", "client is closed")
        if declare:
            self.declare(queue)
        consumer = Consumer(
            self,
            queue,
            handler,
            prefetch=prefetch,
            lease_ms=lease_ms,
            wait_ms=wait_ms,
            parse=parse,
            on_error=on_error,
            max_attempts=max_attempts,
            shutdown_timeout=shutdown_timeout,
        )
        consumer.start()
        self._consumers.add(consumer)
        return consumer

    def run(
        self,
        queue: str,
        handler: Handler,
        **options: Any,
    ) -> None:
        consumer = self.consume(queue, handler, **options)
        try:
            consumer.wait()
        except KeyboardInterrupt:
            pass
        finally:
            consumer.stop()

    def _forget(self, consumer: Any) -> None:
        self._consumers.discard(consumer)

    def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        for consumer in list(self._consumers):
            consumer.stop()
        self._consumers.clear()
        self._control.close()
        if self._process is not None:
            _stop_process(self._process, 6.0)
            self._process = None


def _text(value: Any) -> str:
    if isinstance(value, (bytes, bytearray)):
        return bytes(value).decode("utf-8")
    return str(value)


def _read_address(process: subprocess.Popen[str], timeout: float) -> tuple[str, int]:
    assert process.stdout is not None
    deadline = time.monotonic() + timeout
    while True:
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise AnteError("STARTUP", "ante did not print a native address in time")
        ready, _, _ = select.select([process.stdout], [], [], remaining)
        if not ready:
            raise AnteError("STARTUP", "ante did not print a native address in time")
        line = process.stdout.readline()
        if not line:
            code = process.poll()
            raise AnteError(
                "STARTUP",
                f"ante exited during startup (code {code})",
            )
        parts = line.split()
        if len(parts) == 2 and parts[0] == "native":
            host, _, port = parts[1].rpartition(":")
            return host, int(port)


def _stop_process(process: subprocess.Popen[str], timeout: float) -> None:
    if process.poll() is not None:
        if process.stdout is not None:
            process.stdout.close()
        return
    process.terminate()
    try:
        process.wait(timeout=timeout)
    except subprocess.TimeoutExpired:
        process.kill()
        try:
            process.wait(timeout=2.0)
        except subprocess.TimeoutExpired:
            pass
    if process.stdout is not None:
        process.stdout.close()

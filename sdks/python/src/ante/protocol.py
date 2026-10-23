from __future__ import annotations

import socket
import threading
from typing import Any

MAX_BULK = 16 * 1024 * 1024
MAX_ARRAY = 2048


class AnteError(RuntimeError):
    def __init__(self, code: str, message: str = "") -> None:
        self.code = code
        self.message = message
        super().__init__(f"{code}: {message}" if message else code)


class RespError:
    __slots__ = ("code", "message")

    def __init__(self, line: str) -> None:
        code, _, message = line.partition(" ")
        self.code = code
        self.message = message

    def __repr__(self) -> str:
        return f"RespError({self.code!r}, {self.message!r})"


def encode(*parts: Any) -> bytes:
    out = bytearray(f"*{len(parts)}\r\n", "utf-8")
    for part in parts:
        if isinstance(part, bytes):
            body = part
        elif isinstance(part, bytearray):
            body = bytes(part)
        else:
            body = str(part).encode("utf-8")
        out += f"${len(body)}\r\n".encode("utf-8")
        out += body
        out += b"\r\n"
    return bytes(out)


class RespReader:
    def __init__(self, stream) -> None:
        self._stream = stream

    def read(self) -> Any:
        kind = self._stream.read(1)
        if not kind:
            raise ConnectionError("connection closed")
        line = self._read_line()
        if kind == b"+":
            return line.decode("utf-8")
        if kind == b"-":
            return RespError(line.decode("utf-8"))
        if kind == b":":
            return int(line)
        if kind == b"$":
            length = int(line)
            if length == -1:
                return None
            if length > MAX_BULK:
                raise ValueError("bulk reply exceeds the protocol limit")
            body = self._stream.read(length + 2)
            if len(body) != length + 2 or body[-2:] != b"\r\n":
                raise ConnectionError("truncated bulk reply")
            return body[:-2]
        if kind == b"*":
            count = int(line)
            if count == -1:
                return None
            if count > MAX_ARRAY:
                raise ValueError("array reply exceeds the protocol limit")
            return [self.read() for _ in range(count)]
        raise ValueError(f"unsupported RESP byte {kind!r}")

    def _read_line(self) -> bytes:
        line = self._stream.readline()
        if not line.endswith(b"\r\n"):
            raise ConnectionError("connection closed")
        return line[:-2]


class Connection:
    def __init__(self, sock: socket.socket, timeout: float) -> None:
        self._sock = sock
        self._sock.settimeout(timeout)
        self._reader = RespReader(sock.makefile("rb"))
        self._lock = threading.Lock()
        self._closed = False

    @classmethod
    def open(
        cls,
        host: str = "127.0.0.1",
        port: int = 7300,
        timeout: float = 10.0,
    ) -> Connection:
        sock = socket.create_connection((host, port), timeout=timeout)
        connection = cls(sock, timeout)
        try:
            greeting = connection.request("HELLO", "1")
        except Exception:
            connection.close()
            raise
        if greeting != "ANTE 1":
            connection.close()
            raise AnteError("VERSION", f"unsupported server greeting: {greeting!r}")
        return connection

    def request(self, *parts: Any) -> Any:
        with self._lock:
            if self._closed:
                raise AnteError("CONNECTION_CLOSED", "connection is closed")
            self._sock.sendall(encode(*parts))
            reply = self._reader.read()
        if isinstance(reply, RespError):
            raise AnteError(reply.code, reply.message)
        return reply

    def interrupt(self) -> None:
        try:
            self._sock.shutdown(socket.SHUT_RDWR)
        except OSError:
            pass

    def close(self) -> None:
        with self._lock:
            if self._closed:
                return
            self._closed = True
            try:
                self._sock.sendall(encode("QUIT"))
            except OSError:
                pass
            try:
                self._sock.shutdown(socket.SHUT_RDWR)
            except OSError:
                pass
            self._sock.close()
import json
import os
import shutil
import socket
import subprocess
import tempfile
import time

BIN = os.environ.get("ANTE_BIN", "ante")


def encode(parts):
    out = bytearray(f"*{len(parts)}\r\n", "utf-8")
    for part in parts:
        body = part if isinstance(part, bytes) else str(part).encode("utf-8")
        out += f"${len(body)}\r\n".encode("utf-8") + body + b"\r\n"
    return bytes(out)


class Reader:
    def __init__(self, sock):
        self.sock = sock
        self.file = sock.makefile("rb")

    def value(self):
        line = self.file.readline()
        if not line.endswith(b"\r\n"):
            raise ConnectionError("connection closed")
        kind, payload = line[:1], line[1:-2]
        if kind == b"+":
            return payload.decode("utf-8")
        if kind == b"-":
            code, _, message = payload.decode("utf-8").partition(" ")
            raise RuntimeError(f"{code}: {message}")
        if kind == b":":
            return int(payload)
        if kind == b"$":
            length = int(payload)
            if length == -1:
                return None
            body = self.file.read(length + 2)
            if len(body) != length + 2:
                raise ConnectionError("truncated bulk reply")
            return body[:-2]
        if kind == b"*":
            count = int(payload)
            if count == -1:
                return None
            return [self.value() for _ in range(count)]
        raise RuntimeError(f"unsupported RESP byte {kind!r}")


def start(db):
    child = subprocess.Popen(
        [BIN, "serve", "--db", db, "--no-amqp", "--native", "127.0.0.1:0"],
        stdout=subprocess.PIPE,
        stderr=None,
        text=True,
    )
    deadline = time.monotonic() + 10
    while time.monotonic() < deadline:
        line = child.stdout.readline()
        if not line:
            raise RuntimeError(f"ante exited during startup (code {child.poll()})")
        parts = line.split()
        if len(parts) == 2 and parts[0] == "native":
            host, _, port = parts[1].rpartition(":")
            return child, host, int(port)
    raise RuntimeError("startup timed out")


def main():
    work = tempfile.mkdtemp(prefix="ante-embed-py-")
    child, host, port = start(os.path.join(work, "ante.redb"))
    sock = socket.create_connection((host, port), timeout=10)
    reader = Reader(sock)
    try:
        sock.sendall(encode(["HELLO", "1"]))
        print("hello", reader.value())
        sock.sendall(encode(["DECLARE", "demo"]))
        print("declare", reader.value())

        body = json.dumps({"lang": "python", "n": 1}).encode("utf-8")
        sock.sendall(encode(["PUBLISH", "demo", body, "application/json"]))
        print("publish", reader.value())

        sock.sendall(encode(["TAKE", "demo", "1", "30000", "5000"]))
        record = reader.value()[0]
        token, message_id, _, _, _, attempts, payload = record
        print(
            "took",
            {
                "token": token.decode(),
                "message_id": int(message_id),
                "attempts": int(attempts),
                "payload": json.loads(payload),
            },
        )

        sock.sendall(encode(["ACK", token]))
        print("ack", reader.value())
        sock.sendall(encode(["INFO", "demo"]))
        print("info", b" ".join(reader.value()).decode())
    finally:
        sock.close()
        child.terminate()
        try:
            child.wait(timeout=6)
        except subprocess.TimeoutExpired:
            child.kill()
        shutil.rmtree(work, ignore_errors=True)


if __name__ == "__main__":
    main()
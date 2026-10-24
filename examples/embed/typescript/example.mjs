import { spawn } from "node:child_process";
import { once } from "node:events";
import net from "node:net";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";

const BIN = process.env.ANTE_BIN ?? "ante";

function encode(parts) {
  const chunks = [Buffer.from(`*${parts.length}\r\n`)];
  for (const part of parts) {
    const body = Buffer.isBuffer(part) ? part : Buffer.from(String(part));
    chunks.push(Buffer.from(`$${body.length}\r\n`), body, Buffer.from("\r\n"));
  }
  return Buffer.concat(chunks);
}

class Reader {
  #buffer = Buffer.alloc(0);
  #waiters = [];

  constructor(socket) {
    socket.on("data", (chunk) => {
      this.#buffer = Buffer.concat([this.#buffer, chunk]);
      this.#pump();
    });
    socket.on("error", (error) => this.#fail(error));
    socket.on("close", () => this.#fail(new Error("connection closed")));
  }

  value() {
    return new Promise((resolve, reject) => {
      this.#waiters.push({ resolve, reject });
      this.#pump();
    });
  }

  #fail(error) {
    for (const waiter of this.#waiters.splice(0)) waiter.reject(error);
  }

  #pump() {
    while (this.#waiters.length > 0) {
      const parsed = this.#parse(0);
      if (!parsed) return;
      this.#buffer = this.#buffer.subarray(parsed.offset);
      const waiter = this.#waiters.shift();
      waiter.resolve(parsed.value);
    }
  }

  #parse(offset) {
    if (offset >= this.#buffer.length) return null;
    const type = this.#buffer[offset];
    const end = this.#buffer.indexOf("\r\n", offset + 1);
    if (end < 0) return null;
    const line = this.#buffer.toString("utf8", offset + 1, end);

    if (type === 0x2b) return { value: line, offset: end + 2 };
    if (type === 0x2d) {
      const split = line.indexOf(" ");
      const code = split < 0 ? line : line.slice(0, split);
      const message = split < 0 ? "" : line.slice(split + 1);
      return { value: { error: code, message }, offset: end + 2 };
    }
    if (type === 0x3a) return { value: Number(line), offset: end + 2 };
    if (type === 0x24) {
      const length = Number(line);
      if (length === -1) return { value: null, offset: end + 2 };
      const start = end + 2;
      if (this.#buffer.length < start + length + 2) return null;
      return { value: this.#buffer.subarray(start, start + length), offset: start + length + 2 };
    }
    if (type === 0x2a) {
      const count = Number(line);
      if (count === -1) return { value: null, offset: end + 2 };
      const items = [];
      let cursor = end + 2;
      for (let i = 0; i < count; i += 1) {
        const item = this.#parse(cursor);
        if (!item) return null;
        items.push(item.value);
        cursor = item.offset;
      }
      return { value: items, offset: cursor };
    }
    throw new Error(`unsupported RESP byte ${type}`);
  }
}

async function start(db) {
  const child = spawn(
    BIN,
    ["serve", "--db", db, "--no-amqp", "--native", "127.0.0.1:0"],
    { stdio: ["ignore", "pipe", "inherit"] },
  );

  const address = await new Promise((resolve, reject) => {
    let buffered = "";
    const timer = setTimeout(() => reject(new Error("startup timed out")), 10_000);
    child.stdout.setEncoding("utf8");
    child.stdout.on("data", (chunk) => {
      buffered += chunk;
      const match = /^native\s+(\S+):(\d+)$/m.exec(buffered);
      if (match) {
        clearTimeout(timer);
        resolve({ host: match[1], port: Number(match[2]) });
      }
    });
    child.once("exit", (code) => {
      clearTimeout(timer);
      reject(new Error(`ante exited during startup (code ${code})`));
    });
  });

  const socket = net.createConnection(address);
  await once(socket, "connect");
  return { child, socket, reader: new Reader(socket) };
}

async function request(socket, reader, parts) {
  socket.write(encode(parts));
  const reply = await reader.value();
  if (reply && typeof reply === "object" && !Array.isArray(reply) && "error" in reply) {
    throw new Error(`${reply.error}: ${reply.message}`);
  }
  return reply;
}

const dir = await mkdtemp(join(tmpdir(), "ante-embed-ts-"));
const { child, socket, reader } = await start(join(dir, "ante.sqlite3"));

try {
  console.log("hello", (await request(socket, reader, ["HELLO", "1"])).toString());
  console.log("declare", await request(socket, reader, ["DECLARE", "demo"]));

  const payload = Buffer.from(JSON.stringify({ lang: "typescript", n: 1 }));
  console.log(
    "publish",
    await request(socket, reader, ["PUBLISH", "demo", payload, "application/json"]),
  );

  const [record] = await request(socket, reader, ["TAKE", "demo", "1", "30000", "5000"]);
  const [token, messageId, , , , attempts, body] = record;
  console.log("took", {
    token: token.toString(),
    messageId: Number(messageId),
    attempts: Number(attempts),
    payload: JSON.parse(body.toString()),
  });

  console.log("ack", await request(socket, reader, ["ACK", token.toString()]));
  const info = await request(socket, reader, ["INFO", "demo"]);
  console.log("info", info.map((v) => v.toString()).join(" "));
} finally {
  socket.end();
  child.kill("SIGTERM");
  await Promise.race([once(child, "exit"), new Promise((r) => setTimeout(r, 6_000))]);
  await rm(dir, { recursive: true, force: true });
}
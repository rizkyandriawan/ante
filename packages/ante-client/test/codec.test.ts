import assert from "node:assert/strict";
import test from "node:test";

import { decodePayload, encodePayload } from "../src/codec.js";

test("encodes objects as JSON", () => {
  const encoded = encodePayload({ task: "resize", id: 42 });
  assert.equal(encoded.contentType, "application/json");
  assert.deepEqual(JSON.parse(encoded.body.toString()), { task: "resize", id: 42 });
});

test("encodes strings as text", () => {
  const encoded = encodePayload("hello");
  assert.equal(encoded.contentType, "text/plain");
  assert.equal(encoded.body.toString(), "hello");
});

test("passes buffers through", () => {
  const body = Buffer.from([1, 2, 3]);
  const encoded = encodePayload(body);
  assert.equal(encoded.contentType, "application/octet-stream");
  assert.equal(encoded.body, body);
});

test("decodes JSON by content type", () => {
  const value = decodePayload<{ ok: boolean }>(
    Buffer.from('{"ok":true}'),
    "application/json",
  );
  assert.deepEqual(value, { ok: true });
});

test("returns text when the content type is not JSON", () => {
  assert.equal(decodePayload(Buffer.from("plain"), "text/plain"), "plain");
});

test("uses a custom decoder", () => {
  const value = decodePayload(Buffer.from("42"), undefined, (raw) =>
    Number(raw.toString()),
  );
  assert.equal(value, 42);
});

const JSON_TYPE = "application/json";

export interface EncodedPayload {
  body: Buffer;
  contentType: string;
}

export function encodePayload(payload: unknown): EncodedPayload {
  if (Buffer.isBuffer(payload)) {
    return { body: payload, contentType: "application/octet-stream" };
  }
  if (payload instanceof Uint8Array) {
    return { body: Buffer.from(payload), contentType: "application/octet-stream" };
  }
  if (typeof payload === "string") {
    return { body: Buffer.from(payload, "utf8"), contentType: "text/plain" };
  }
  return {
    body: Buffer.from(JSON.stringify(payload), "utf8"),
    contentType: JSON_TYPE,
  };
}

export function decodePayload<T>(
  raw: Buffer,
  contentType: string | undefined,
  parse?: (raw: Buffer, contentType?: string) => T,
): T {
  if (parse) {
    return parse(raw, contentType);
  }
  if (contentType?.includes("json")) {
    return JSON.parse(raw.toString("utf8")) as T;
  }
  return raw.toString("utf8") as T;
}

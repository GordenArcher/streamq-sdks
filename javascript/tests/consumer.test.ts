// Tests for Consumer URL building, model deserialisation, and error types.
//
// We don't test the live WebSocket/SSE connection here, that would require
// a real or mock WebSocket server which adds significant complexity.
// The connection logic is tested via integration tests against a real broker.
// Unit tests focus on what we can verify without network I/O:
//   - URL construction (group, fromOffset, protocol, https→wss)
//   - Message deserialisation (base64 payload, timestamp, missing fields)
//   - Error class hierarchy (instanceof checks)

import { describe, it, expect } from "vitest";
import { Consumer } from "../src/consumer.js";
import {
  messageFromJSON,
  topicInfoFromJSON,
  publishResultFromJSON,
} from "../src/models.js";
import {
  StreamqError,
  StreamqNotFound,
  StreamqConflict,
  StreamqAPIError,
  StreamqConsumerClosed,
  StreamqMaxReconnects,
} from "../src/errors.js";

describe("Consumer._buildUrl", () => {
  it("builds WebSocket URL with group and fromOffset", () => {
    const consumer = new Consumer("http://localhost:8080", "payments", {
      group: "billing",
      fromOffset: 0,
      protocol: "ws",
    });

    const url = consumer._buildUrl("ws");
    expect(url).toContain("ws://localhost:8080");
    expect(url).toContain("/topics/payments/subscribe/ws");
    expect(url).toContain("group=billing");
    expect(url).toContain("from_offset=0");
  });

  it("builds SSE URL without group or fromOffset for tail", () => {
    const consumer = new Consumer("http://localhost:8080", "events", {
      fromOffset: -1, // tail, should not appear in URL
      protocol: "sse",
    });

    const url = consumer._buildUrl("sse");
    expect(url).toContain("http://localhost:8080");
    expect(url).toContain("/topics/events/subscribe/sse");
    expect(url).not.toContain("group");
    expect(url).not.toContain("from_offset");
  });

  it("replaces https with wss for WebSocket", () => {
    const consumer = new Consumer("https://broker.example.com", "t", {
      protocol: "ws",
    });
    expect(consumer._buildUrl("ws")).toMatch(/^wss:\/\//);
  });

  it("keeps https for SSE", () => {
    const consumer = new Consumer("https://broker.example.com", "t", {
      protocol: "sse",
    });
    expect(consumer._buildUrl("sse")).toMatch(/^https:\/\//);
  });

  it("strips trailing slash from baseUrl", () => {
    const consumer = new Consumer("http://localhost:8080/", "payments", {});
    const url = consumer._buildUrl("sse");
    expect(url).not.toContain("//topics");
  });

  it("includes fromOffset=0 in URL (not treated as falsy)", () => {
    const consumer = new Consumer("http://localhost:8080", "payments", {
      fromOffset: 0,
    });
    const url = consumer._buildUrl("ws");
    expect(url).toContain("from_offset=0");
  });

  it("does not include from_offset when offset is -1", () => {
    const consumer = new Consumer("http://localhost:8080", "payments", {
      fromOffset: -1,
    });
    const url = consumer._buildUrl("ws");
    expect(url).not.toContain("from_offset");
  });
});

describe("messageFromJSON", () => {
  it("decodes base64 payload to Uint8Array", () => {
    // "hello" in base64 is "aGVsbG8="
    const msg = messageFromJSON({
      id: "1",
      offset: 1,
      topic: "t",
      payload: "aGVsbG8=",
      timestamp: "2025-01-01T00:00:00Z",
    });

    expect(new TextDecoder().decode(msg.payload)).toBe("hello");
  });

  it("parses timestamp as Date", () => {
    const msg = messageFromJSON({
      id: "0",
      offset: 0,
      topic: "t",
      payload: "",
      timestamp: "2025-06-15T12:30:00Z",
    });

    expect(msg.timestamp).toBeInstanceOf(Date);
    expect(msg.timestamp.getUTCFullYear()).toBe(2025);
    expect(msg.timestamp.getUTCMonth()).toBe(5); // June = 5 (0-indexed)
  });

  it("handles empty payload gracefully", () => {
    const msg = messageFromJSON({
      id: "0",
      offset: 0,
      topic: "t",
      payload: "",
      timestamp: "2025-01-01T00:00:00Z",
    });
    expect(msg.payload).toBeInstanceOf(Uint8Array);
    expect(msg.payload.length).toBe(0);
  });

  it("handles missing fields with defaults", () => {
    const msg = messageFromJSON({});
    expect(msg.id).toBe("");
    expect(msg.offset).toBe(0);
    expect(msg.topic).toBe("");
    expect(msg.payload).toBeInstanceOf(Uint8Array);
  });

  it("returns correct offset value", () => {
    const msg = messageFromJSON({
      id: "42",
      offset: 42,
      topic: "payments",
      payload: "",
      timestamp: "2025-01-01T00:00:00Z",
    });
    expect(msg.offset).toBe(42);
  });
});

describe("topicInfoFromJSON", () => {
  it("maps snake_case broker fields to camelCase", () => {
    const info = topicInfoFromJSON({
      name: "payments",
      message_count: 100,
      subscriber_count: 3,
      latest_offset: 99,
    });

    expect(info.name).toBe("payments");
    expect(info.messageCount).toBe(100);
    expect(info.subscriberCount).toBe(3);
    expect(info.latestOffset).toBe(99);
  });

  it("defaults latestOffset to -1 for empty topic", () => {
    const info = topicInfoFromJSON({ name: "empty" });
    expect(info.latestOffset).toBe(-1);
  });
});

describe("publishResultFromJSON", () => {
  it("parses offset and topic", () => {
    const result = publishResultFromJSON({
      offset: 7,
      topic: "payments",
      timestamp: "2025-01-01T00:00:00Z",
    });

    expect(result.offset).toBe(7);
    expect(result.topic).toBe("payments");
    expect(result.timestamp).toBeInstanceOf(Date);
  });
});

describe("Error classes", () => {
  it("StreamqNotFound instanceof StreamqError", () => {
    const err = new StreamqNotFound("not found");
    expect(err instanceof StreamqError).toBe(true);
    expect(err instanceof StreamqNotFound).toBe(true);
    expect(err.name).toBe("StreamqNotFound");
  });

  it("StreamqConflict instanceof StreamqError", () => {
    const err = new StreamqConflict("conflict");
    expect(err instanceof StreamqError).toBe(true);
    expect(err instanceof StreamqConflict).toBe(true);
  });

  it("StreamqAPIError carries statusCode", () => {
    const err = new StreamqAPIError(500, "server error");
    expect(err instanceof StreamqError).toBe(true);
    expect(err.statusCode).toBe(500);
    expect(err.message).toContain("500");
  });

  it("StreamqConsumerClosed instanceof StreamqError", () => {
    const err = new StreamqConsumerClosed();
    expect(err instanceof StreamqError).toBe(true);
  });

  it("StreamqMaxReconnects carries attempts and lastError", () => {
    const cause = new Error("connection refused");
    const err = new StreamqMaxReconnects(3, cause);
    expect(err instanceof StreamqError).toBe(true);
    expect(err.attempts).toBe(3);
    expect(err.lastError).toBe(cause);
    expect(err.message).toContain("3");
  });
});

describe("Consumer.ack", () => {
  it("is a no-op for SSE consumers", () => {
    const consumer = new Consumer("http://localhost:8080", "t", {
      protocol: "sse",
      group: "billing",
    });
    // Should not throw even though there's no WebSocket open.
    expect(() => consumer.ack(42)).not.toThrow();
  });

  it("is a no-op for ungrouped consumers", () => {
    const consumer = new Consumer("http://localhost:8080", "t", {
      protocol: "ws",
      group: "", // no group
    });
    expect(() => consumer.ack(42)).not.toThrow();
  });
});

describe("Consumer.close", () => {
  it("is idempotent", () => {
    const consumer = new Consumer("http://localhost:8080", "t");
    expect(() => {
      consumer.close();
      consumer.close();
      consumer.close();
    }).not.toThrow();
  });
});

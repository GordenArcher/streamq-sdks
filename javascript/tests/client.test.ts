// Tests for StreamqClient using vitest's built-in fetch mocking.
// No real broker required.

import { describe, it, expect, vi, beforeEach, afterEach } from "vitest";
import { StreamqClient } from "../src/client.js";
import {
  StreamqConflict,
  StreamqNotFound,
  StreamqAPIError,
} from "../src/errors.js";

function mockFetch(status: number, data: unknown) {
  return vi.spyOn(globalThis, "fetch").mockResolvedValueOnce({
    status,
    json: async () => data,
    ok: status >= 200 && status < 300,
  } as Response);
}

function brokerSuccess(data: unknown, status = 200) {
  return { status: "success", data };
}

function brokerError(message: string, status: number) {
  return mockFetch(status, { status: "error", message });
}

let client: StreamqClient;

beforeEach(() => {
  client = new StreamqClient("http://localhost:8080");
});

afterEach(() => {
  vi.restoreAllMocks();
});

describe("StreamqClient", () => {
  it("throws if baseUrl is empty", () => {
    expect(() => new StreamqClient("")).toThrow();
  });

  it("strips trailing slash from baseUrl", () => {
    const c = new StreamqClient("http://localhost:8080/");
    expect((c as any).baseUrl).toBe("http://localhost:8080");
  });
});

describe("createTopic", () => {
  it("succeeds on 201", async () => {
    mockFetch(201, brokerSuccess({ name: "payments" }));
    await expect(client.createTopic("payments")).resolves.toBeUndefined();
  });

  it("throws StreamqConflict on 409", async () => {
    brokerError("topic already exists", 409);
    await expect(client.createTopic("payments")).rejects.toThrow(
      StreamqConflict,
    );
  });

  it("sends correct method and path", async () => {
    const spy = mockFetch(201, brokerSuccess({ name: "payments" }));
    await client.createTopic("payments");
    expect(spy).toHaveBeenCalledWith(
      "http://localhost:8080/topics",
      expect.objectContaining({ method: "POST" }),
    );
  });
});

describe("deleteTopic", () => {
  it("succeeds on 200", async () => {
    mockFetch(200, brokerSuccess(null));
    await expect(client.deleteTopic("payments")).resolves.toBeUndefined();
  });

  it("throws StreamqNotFound on 404", async () => {
    brokerError("topic not found", 404);
    await expect(client.deleteTopic("missing")).rejects.toThrow(
      StreamqNotFound,
    );
  });
});

describe("listTopics", () => {
  it("returns parsed TopicInfo array", async () => {
    mockFetch(
      200,
      brokerSuccess([
        {
          name: "payments",
          message_count: 10,
          subscriber_count: 2,
          latest_offset: 9,
        },
        {
          name: "events",
          message_count: 5,
          subscriber_count: 0,
          latest_offset: 4,
        },
      ]),
    );

    const topics = await client.listTopics();
    expect(topics).toHaveLength(2);
    expect(topics[0]?.name).toBe("payments");
    expect(topics[0]?.messageCount).toBe(10);
    expect(topics[1]?.name).toBe("events");
  });

  it("returns empty array when no topics", async () => {
    mockFetch(200, brokerSuccess([]));
    const topics = await client.listTopics();
    expect(topics).toEqual([]);
  });
});

describe("getTopic", () => {
  it("returns parsed TopicInfo", async () => {
    mockFetch(
      200,
      brokerSuccess({
        name: "payments",
        message_count: 42,
        subscriber_count: 1,
        latest_offset: 41,
      }),
    );

    const info = await client.getTopic("payments");
    expect(info.name).toBe("payments");
    expect(info.latestOffset).toBe(41);
  });

  it("throws StreamqNotFound on 404", async () => {
    brokerError("topic not found", 404);
    await expect(client.getTopic("missing")).rejects.toThrow(StreamqNotFound);
  });
});

describe("publish", () => {
  it("returns PublishResult with offset", async () => {
    mockFetch(
      202,
      brokerSuccess({
        offset: 7,
        topic: "payments",
        timestamp: "2025-01-01T00:00:00Z",
      }),
    );

    const payload = new TextEncoder().encode('{"amount":100}');
    const result = await client.publish("payments", payload);

    expect(result.offset).toBe(7);
    expect(result.topic).toBe("payments");
  });

  it("base64-encodes payload in request body", async () => {
    const spy = mockFetch(
      202,
      brokerSuccess({
        offset: 0,
        topic: "payments",
        timestamp: "2025-01-01T00:00:00Z",
      }),
    );

    const payload = new TextEncoder().encode("hello");
    await client.publish("payments", payload);

    const call = spy.mock.calls[0];
    const body = JSON.parse(call?.[1]?.body as string) as { payload: string };
    // "hello" base64-encodes to "aGVsbG8="
    expect(body.payload).toBe("aGVsbG8=");
  });

  it("throws StreamqNotFound on 404", async () => {
    brokerError("topic not found", 404);
    await expect(
      client.publish("missing", new Uint8Array([1, 2, 3])),
    ).rejects.toThrow(StreamqNotFound);
  });
});

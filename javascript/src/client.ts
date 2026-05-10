// StreamqClient — main entry point for the streamq JavaScript SDK.
//
// Wraps the broker's HTTP API for topic management and publishing,
// and provides a subscribe() method that returns a Consumer.
//
// Uses native fetch() — available in all modern browsers and Node.js 18+.
// No axios, no node-fetch. Zero extra HTTP dependencies.
//
// Usage:
//
//   const client = new StreamqClient("http://localhost:8080");
//
//   await client.createTopic("payments");
//
//   const result = await client.publish("payments",
//     new TextEncoder().encode('{"amount":100}')
//   );
//
//   const consumer = client.subscribe("payments", {
//     group: "billing",
//     fromOffset: 0,
//   });
//
//   for await (const msg of consumer) {
//     console.log(new TextDecoder().decode(msg.payload));
//     consumer.ack(msg.offset);
//   }

import { StreamqAPIError, StreamqConflict, StreamqNotFound } from "./errors.js";
import {
  PublishResult,
  TopicInfo,
  publishResultFromJSON,
  topicInfoFromJSON,
} from "./models.js";
import { Consumer, ConsumerOptions } from "./consumer.js";

export interface ClientOptions {
  /**
   * Timeout in milliseconds for individual HTTP API calls.
   * Does not apply to long-lived streaming connections.
   * Default: 10000 (10s).
   */
  timeout?: number;

  /**
   * Seconds to wait between consumer reconnect attempts.
   * Default: 2.
   */
  reconnectDelay?: number;

  /**
   * Max consumer reconnect attempts before giving up.
   * 0 = retry forever. Default: 0.
   */
  maxReconnectAttempts?: number;
}

export class StreamqClient {
  private readonly baseUrl: string;
  private readonly timeout: number;
  private readonly reconnectDelay: number;
  private readonly maxReconnectAttempts: number;

  constructor(baseUrl: string, options: ClientOptions = {}) {
    if (!baseUrl) throw new Error("streamq: baseUrl is required");
    this.baseUrl = baseUrl.replace(/\/$/, "");
    this.timeout = options.timeout ?? 10_000;
    this.reconnectDelay = options.reconnectDelay ?? 2;
    this.maxReconnectAttempts = options.maxReconnectAttempts ?? 0;
  }

  /**
   * Create a new topic on the broker.
   *
   * @throws {StreamqConflict} if the topic already exists.
   * @throws {StreamqAPIError} for any other broker error.
   */
  async createTopic(name: string): Promise<void> {
    await this._request("POST", "/topics", { name }, 201);
  }

  /**
   * Delete a topic and disconnect all its subscribers.
   *
   * @throws {StreamqNotFound} if the topic does not exist.
   */
  async deleteTopic(name: string): Promise<void> {
    await this._request("DELETE", `/topics/${name}`, null, 200);
  }

  /**
   * Return stats for all topics.
   * Returns an empty array if no topics exist.
   */
  async listTopics(): Promise<TopicInfo[]> {
    const body = await this._request("GET", "/topics", null, 200);
    const data = (body["data"] as Record<string, unknown>[] | null) ?? [];
    return data.map(topicInfoFromJSON);
  }

  /**
   * Return stats for a single topic.
   *
   * @throws {StreamqNotFound} if the topic does not exist.
   */
  async getTopic(name: string): Promise<TopicInfo> {
    const body = await this._request("GET", `/topics/${name}`, null, 200);
    return topicInfoFromJSON(body["data"] as Record<string, unknown>);
  }

  /**
   * Publish a message to the named topic.
   *
   * @param topic - Topic name.
   * @param payload - Raw bytes to publish. Use TextEncoder for strings:
   *   new TextEncoder().encode('{"amount": 100}')
   *
   * @returns PublishResult with the assigned offset and server timestamp.
   * @throws {StreamqNotFound} if the topic does not exist.
   */
  async publish(topic: string, payload: Uint8Array): Promise<PublishResult> {
    // Base64-encode for JSON transport.
    // btoa() requires a binary string, we convert Uint8Array first.
    const binary = Array.from(payload)
      .map((b) => String.fromCharCode(b))
      .join("");
    const encoded = btoa(binary);

    const body = await this._request(
      "POST",
      `/topics/${topic}/publish`,
      { payload: encoded },
      202,
    );
    return publishResultFromJSON(body["data"] as Record<string, unknown>);
  }

  /**
   * Create a Consumer for the given topic.
   *
   * Returns immediately — the connection is established lazily when
   * iteration begins.
   *
   * @example
   * ```ts
   * const consumer = client.subscribe("payments", {
   *   group: "billing",
   *   fromOffset: 0,
   * });
   *
   * for await (const msg of consumer) {
   *   console.log(new TextDecoder().decode(msg.payload));
   *   consumer.ack(msg.offset);
   * }
   * ```
   */
  subscribe(topic: string, options: ConsumerOptions = {}): Consumer {
    return new Consumer(this.baseUrl, topic, {
      reconnectDelay: this.reconnectDelay,
      maxReconnectAttempts: this.maxReconnectAttempts,
      ...options, // caller options override client defaults
    });
  }

  private async _request(
    method: string,
    path: string,
    body: Record<string, unknown> | null,
    expectedStatus: number,
  ): Promise<Record<string, unknown>> {
    const controller = new AbortController();
    const timer = setTimeout(() => controller.abort(), this.timeout);

    try {
      const init: RequestInit = {
        method,
        headers: body ? { "Content-Type": "application/json" } : {},
        signal: controller.signal,
      };
      if (body) {
        init.body = JSON.stringify(body);
      }

      const res = await fetch(this.baseUrl + path, init);

      // Parse JSON regardless of status, the broker always returns JSON.
      const data = (await res.json()) as Record<string, unknown>;

      if (res.status !== expectedStatus) {
        const message = String(data["message"] ?? `HTTP ${res.status}`);
        if (res.status === 404) throw new StreamqNotFound(message);
        if (res.status === 409) throw new StreamqConflict(message);
        throw new StreamqAPIError(res.status, message);
      }

      return data;
    } catch (err) {
      if (
        err instanceof StreamqAPIError ||
        err instanceof StreamqNotFound ||
        err instanceof StreamqConflict
      ) {
        throw err;
      }
      // Network error, timeout, etc.
      throw new StreamqAPIError(0, (err as Error).message ?? "network error");
    } finally {
      clearTimeout(timer);
    }
  }
}

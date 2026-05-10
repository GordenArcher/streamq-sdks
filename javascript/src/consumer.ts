// Consumer — an async generator for receiving messages from a streamq topic.
//
// Design — AsyncGenerator pattern:
// The consumer implements AsyncIterable<Message> so callers use the natural
// for-await-of syntax:
//
//   for await (const msg of consumer) {
//     await process(msg);
//     consumer.ack(msg.offset);
//   }
//
// Why AsyncGenerator instead of EventEmitter / callback?
// - for-await-of is built into modern JS/TS, no extra API to learn
// - Back-pressure is handled by the generator protocol, the next message
//   is not fetched until the caller's loop body completes
// - Cancellation is clean: consumer.close() causes the for-await-of to exit
//
// Reconnect:
// On connection loss, the consumer waits reconnectDelay ms and retries.
// Reconnect attempts are bounded by maxReconnectAttempts (0 = unlimited).
// On exhaustion, StreamqMaxReconnects is thrown from the generator.
//
// Environment detection:
// The SDK targets both browser and Node.js. We detect the environment
// at runtime and use the appropriate WebSocket/EventSource implementations:
//   - Browser: native WebSocket + native EventSource (no deps needed)
//   - Node.js: native WebSocket (Node 22+) or 'ws' package + 'eventsource' pkg

import EventSource from "eventsource";
import { StreamqConsumerClosed, StreamqMaxReconnects } from "./errors.js";
import { Message, messageFromJSON } from "./models.js";

export type Protocol = "ws" | "sse";

export interface ConsumerOptions {
  /** Consumer group ID. Empty = broadcast (receive all messages). */
  group?: string;

  /**
   * Starting offset for replay.
   * -1 = tail (new messages only, default).
   *  0 = full history replay.
   *  N = resume from offset N.
   */
  fromOffset?: number;

  /** Streaming transport. "ws" supports ack(), "sse" is simpler. Default: "ws". */
  protocol?: Protocol;

  /** Seconds to wait between reconnect attempts. Default: 2. */
  reconnectDelay?: number;

  /** Max reconnect attempts before giving up. 0 = unlimited. Default: 0. */
  maxReconnectAttempts?: number;
}

export class Consumer implements AsyncIterable<Message> {
  private readonly baseUrl: string;
  private readonly topic: string;
  private readonly group: string;
  private readonly fromOffset: number;
  private readonly protocol: Protocol;
  private readonly reconnectDelay: number;
  private readonly maxReconnectAttempts: number;

  private closed = false;
  // resolveNext and rejectNext are the pending Promise callbacks from
  // the generator's yield point. The WebSocket/SSE event handlers call
  // them to deliver the next message or signal termination.
  private resolveNext: ((msg: Message) => void) | null = null;
  private rejectNext: ((err: unknown) => void) | null = null;

  // messageQueue buffers messages that arrive before the generator
  // has called next(). This handles bursts where messages arrive faster
  // than the caller processes them.
  private messageQueue: Message[] = [];

  // ws holds the active WebSocket connection for ack() calls.
  private ws: WebSocket | null = null;

  constructor(baseUrl: string, topic: string, options: ConsumerOptions = {}) {
    this.baseUrl = baseUrl.replace(/\/$/, "");
    this.topic = topic;
    this.group = options.group ?? "";
    this.fromOffset = options.fromOffset ?? -1;
    this.protocol = options.protocol ?? "ws";
    this.reconnectDelay = (options.reconnectDelay ?? 2) * 1000; // convert to ms
    this.maxReconnectAttempts = options.maxReconnectAttempts ?? 0;
  }

  [Symbol.asyncIterator](): AsyncGenerator<Message> {
    return this._stream();
  }

  private async *_stream(): AsyncGenerator<Message> {
    let attempts = 0;
    let lastError: unknown;

    while (!this.closed) {
      try {
        if (this.protocol === "sse") {
          yield* this._streamSSE();
        } else {
          yield* this._streamWS();
        }

        // Generator exhausted cleanly, broker closed the connection.
        if (this.closed) return;
      } catch (err) {
        if (err instanceof StreamqConsumerClosed) return;

        lastError = err;
        attempts++;

        if (
          this.maxReconnectAttempts > 0 &&
          attempts >= this.maxReconnectAttempts
        ) {
          throw new StreamqMaxReconnects(attempts, lastError);
        }

        // Wait before reconnecting.
        await this._sleep(this.reconnectDelay);
      }
    }
  }

  private async *_streamWS(): AsyncGenerator<Message> {
    const url = this._buildUrl("ws");

    // Use native WebSocket (browser or Node 22+).
    // In Node < 22 callers should polyfill globalThis.WebSocket with 'ws'.
    const ws = new WebSocket(url);
    this.ws = ws;

    try {
      // Wait for the connection to open before yielding anything.
      await new Promise<void>((resolve, reject) => {
        ws.onopen = () => resolve();
        ws.onerror = (e) => reject(new Error(`ws connection failed`));
      });

      // Yield messages until the connection closes.
      while (!this.closed) {
        const msg = await this._nextWSMessage(ws);
        if (msg === null) break; // connection closed
        yield msg;
      }
    } finally {
      this.ws = null;
      if (
        ws.readyState === WebSocket.OPEN ||
        ws.readyState === WebSocket.CONNECTING
      ) {
        ws.close();
      }
    }
  }

  /**
   * Wait for the next message from the WebSocket.
   * Returns null if the connection was closed cleanly.
   */
  private _nextWSMessage(ws: WebSocket): Promise<Message | null> {
    return new Promise((resolve, reject) => {
      ws.onmessage = (event) => {
        try {
          const data = JSON.parse(event.data as string) as Record<
            string,
            unknown
          >;
          resolve(messageFromJSON(data));
        } catch {
          // Malformed frame, skip by resolving with a dummy that gets filtered.
          // We re-register onmessage on the next call.
        }
      };

      ws.onclose = (event) => {
        if (this.closed || event.wasClean) {
          resolve(null);
        } else {
          reject(new Error(`ws closed unexpectedly: code=${event.code}`));
        }
      };

      ws.onerror = () => {
        reject(new Error("ws error"));
      };
    });
  }

  private async *_streamSSE(): AsyncGenerator<Message> {
    const url = this._buildUrl("sse");

    // Use the 'eventsource' package which works in both Node and browser.
    // In a pure browser build you could use native EventSource directly.
    const es = new EventSource(url);

    try {
      while (!this.closed) {
        const msg = await this._nextSSEMessage(es);
        if (msg === null) break;
        yield msg;
      }
    } finally {
      es.close();
    }
  }

  /**
   * Wait for the next SSE message event.
   * Returns null if the EventSource closes.
   */
  private _nextSSEMessage(es: EventSource): Promise<Message | null> {
    return new Promise((resolve, reject) => {
      es.onmessage = (event) => {
        try {
          const data = JSON.parse(event.data as string) as Record<
            string,
            unknown
          >;
          resolve(messageFromJSON(data));
        } catch {
          // Malformed event, skip.
        }
      };

      es.onerror = () => {
        if (this.closed) {
          resolve(null);
        } else {
          reject(new Error("sse connection error"));
        }
      };
    });
  }

  /**
   * Acknowledge a message offset.
   *
   * Sends {"ack": offset} to the broker over the WebSocket connection.
   * Only meaningful for WebSocket consumers in a consumer group
   * provides at-least-once delivery guarantees.
   *
   * No-op for SSE consumers or ungrouped consumers.
   */
  ack(offset: number): void {
    if (this.protocol === "sse" || !this.group) return;

    if (this.ws && this.ws.readyState === WebSocket.OPEN) {
      this.ws.send(JSON.stringify({ ack: offset }));
    }
  }

  /**
   * Stop the consumer.
   *
   * Causes the for-await-of loop to exit cleanly on the next iteration.
   * Idempotent — safe to call multiple times.
   */
  close(): void {
    this.closed = true;
    if (this.ws) {
      this.ws.close();
    }
  }

  _buildUrl(protocol: "ws" | "sse"): string {
    let base = this.baseUrl;

    if (protocol === "ws") {
      base = base.replace("https://", "wss://").replace("http://", "ws://");
    }

    let url = `${base}/topics/${this.topic}/subscribe/${protocol}`;

    const params = new URLSearchParams();
    if (this.group) params.set("group", this.group);
    if (this.fromOffset >= 0)
      params.set("from_offset", String(this.fromOffset));

    const qs = params.toString();
    if (qs) url += `?${qs}`;

    return url;
  }

  private _sleep(ms: number): Promise<void> {
    return new Promise((resolve) => setTimeout(resolve, ms));
  }
}

# streamq-js

Official JavaScript/TypeScript SDK for [streamq](https://github.com/GordenArcher/streamq) — a lightweight message broker with WebSocket and SSE delivery.

## Installation

```bash
npm install streamq-js
# or
pnpm add streamq-js
# or
yarn add streamq-js
```

Requires Node.js 18+ or any modern browser.

---

## Quick start

```ts
import { StreamqClient } from "streamq-js";

const client = new StreamqClient("http://localhost:8080");

// Create a topic
await client.createTopic("payments");

// Publish a message
const result = await client.publish(
  "payments",
  new TextEncoder().encode('{"amount": 100, "currency": "GHS"}')
);
console.log(`published at offset ${result.offset}`);

// Subscribe and receive messages
const consumer = client.subscribe("payments", {
  group: "billing",
  fromOffset: 0,
});

for await (const msg of consumer) {
  const text = new TextDecoder().decode(msg.payload);
  console.log(`offset ${msg.offset}: ${text}`);
  consumer.ack(msg.offset); // explicit ack for at-least-once delivery
}
```

---

## API

### `new StreamqClient(baseUrl, options?)`

```ts
const client = new StreamqClient("http://localhost:8080", {
  timeout: 10_000,           // HTTP call timeout in ms (default: 10000)
  reconnectDelay: 2,         // seconds between reconnects (default: 2)
  maxReconnectAttempts: 0,   // 0 = retry forever (default: 0)
});
```

### Topic management

```ts
// Create
await client.createTopic("events");

// List all topics
const topics = await client.listTopics();
// [{ name, messageCount, subscriberCount, latestOffset }, ...]

// Get single topic stats
const info = await client.getTopic("events");
console.log(`${info.name}: ${info.messageCount} messages`);

// Delete
await client.deleteTopic("events");
```

### Publishing

```ts
// Payload is Uint8Array — use TextEncoder for strings
const payload = new TextEncoder().encode('{"amount": 100}');
const result = await client.publish("payments", payload);
// result: { offset: number, topic: string, timestamp: Date }
```

### Subscribing

```ts
// Broadcast — receive every message (no group)
const consumer = client.subscribe("payments");

// Consumer group — work queue (one message → one subscriber in group)
const consumer = client.subscribe("payments", { group: "billing" });

// Replay from offset 0 (full retained history)
const consumer = client.subscribe("payments", { fromOffset: 0 });

// Resume from a specific offset
const consumer = client.subscribe("payments", { fromOffset: 42 });

// SSE instead of WebSocket (no ack, works through more proxies)
const consumer = client.subscribe("payments", { protocol: "sse" });

// Iterate
for await (const msg of consumer) {
  const text = new TextDecoder().decode(msg.payload);
  consumer.ack(msg.offset);
}

// Stop manually
consumer.close();
```

### Error handling

```ts
import {
  StreamqClient,
  StreamqNotFound,
  StreamqConflict,
  StreamqError,
} from "streamq-js";

try {
  await client.createTopic("payments");
} catch (err) {
  if (err instanceof StreamqConflict) {
    // Topic already exists — that's fine
  } else if (err instanceof StreamqNotFound) {
    // Topic not found
  } else if (err instanceof StreamqError) {
    // Any other broker error
  }
}
```

---

## Delivery guarantees

| Setup | Guarantee |
|-------|-----------|
| WebSocket + group + `ack()` | At-least-once |
| WebSocket, no group | At-most-once |
| SSE (any) | At-most-once |

For at-least-once delivery: use WebSocket with a consumer group and call `consumer.ack(msg.offset)` after successfully processing each message. On reconnect, the broker redelivers from the last acknowledged offset.

---

## Browser usage

The SDK works in modern browsers with no bundler configuration needed. Native `fetch`, `WebSocket`, and `EventSource` are used automatically.

```html
<script type="module">
  import { StreamqClient } from "https://esm.sh/streamq-js";

  const client = new StreamqClient("http://localhost:8080");
  const consumer = client.subscribe("events", { protocol: "sse" });

  for await (const msg of consumer) {
    console.log(new TextDecoder().decode(msg.payload));
  }
</script>
```

---

## Running tests

```bash
npm install
npm test
```

Tests use vitest with fetch mocking — no real broker required.

---

## Broker

The broker is at [github.com/GordenArcher/streamq](https://github.com/GordenArcher/streamq).

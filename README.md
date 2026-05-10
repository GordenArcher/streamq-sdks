# streamq-sdks

Official client SDKs for [streamq](https://github.com/GordenArcher/streamq) — a lightweight message broker with WebSocket and SSE delivery, Prometheus metrics, and WAL-backed persistence.

## SDKs

| Language | Package | Install |
|----------|---------|---------|
| Go | `github.com/GordenArcher/streamq-sdks/go` | `go get github.com/GordenArcher/streamq-sdks/go` |
| Python | `streamq-python` | `pip install streamq-python` |
| JavaScript / TypeScript | `streamq-js` | `npm install streamq-js` |
| Rust | `streamq-rs` | `cargo add streamq-rs` |

Each SDK lives in its own subdirectory with its own README, package config, and test suite. No real broker is required to run the tests — all SDKs use in-process mock servers.

---

## Quick starts

### Go

```go
import streamq "github.com/GordenArcher/streamq-sdks/go"

client, _ := streamq.NewClient("http://localhost:8080")
defer client.Close()

client.CreateTopic(ctx, "payments")

result, _ := client.Publish(ctx, "payments", []byte(`{"amount":100}`))
fmt.Printf("published at offset %d\n", result.Offset)

consumer, _ := client.Subscribe(ctx, "payments",
    streamq.WithGroup("billing"),
    streamq.WithFromOffset(0),
)
for msg := range consumer.Messages() {
    fmt.Printf("offset %d: %s\n", msg.Offset, msg.Payload)
    consumer.Ack(msg.Offset)
}
```

### Python (async)

```python
from streamq import AsyncClient

async with AsyncClient("http://localhost:8080") as client:
    await client.create_topic("payments")
    await client.publish("payments", b'{"amount": 100}')

    async with client.subscribe("payments", group="billing", from_offset=0) as consumer:
        async for msg in consumer:
            print(f"offset {msg.offset}: {msg.payload}")
            await consumer.ack(msg.offset)
```

### Python (sync)

```python
from streamq import SyncClient

client = SyncClient("http://localhost:8080")
client.create_topic("payments")
client.publish("payments", b'{"amount": 100}')

with client.subscribe("payments", group="billing", from_offset=0) as consumer:
    for msg in consumer:
        print(f"offset {msg.offset}: {msg.payload}")
        consumer.ack(msg.offset)
```

### JavaScript / TypeScript

```ts
import { StreamqClient } from "streamq-js";

const client = new StreamqClient("http://localhost:8080");

await client.createTopic("payments");
await client.publish("payments", new TextEncoder().encode('{"amount":100}'));

const consumer = client.subscribe("payments", {
    group: "billing",
    fromOffset: 0,
});

for await (const msg of consumer) {
    console.log(`offset ${msg.offset}:`, new TextDecoder().decode(msg.payload));
    consumer.ack(msg.offset);
}
```

### Rust

```rust
use streamq::{StreamqClient, ConsumerOptions};

#[tokio::main]
async fn main() -> streamq::Result<()> {
    let client = StreamqClient::new("http://localhost:8080")?;

    client.create_topic("payments").await?;
    let result = client.publish("payments", b"{\"amount\": 100}").await?;
    println!("published at offset {}", result.offset);

    let mut consumer = client.subscribe("payments", ConsumerOptions {
        group: "billing".to_string(),
        from_offset: 0,
        ..Default::default()
    });

    while let Some(msg) = consumer.next().await? {
        println!("offset {}: {:?}", msg.offset, msg.payload);
        consumer.ack(msg.offset);
    }

    Ok(())
}
```

---

## Delivery guarantees

| Setup | Guarantee | Notes |
|-------|-----------|-------|
| WebSocket + group + `ack()` | At-least-once | Broker redelivers from last committed offset on reconnect |
| WebSocket, no group | At-most-once | Auto-committed on send |
| SSE (any) | At-most-once | Unidirectional — no ack channel |

---

## Consumer API comparison

Each SDK exposes the same semantics with idiomatic syntax for its language:

| Feature | Go | Python | JavaScript | Rust |
|---------|----|--------|------------|------|
| Consumer style | Channel (`range`) | Async generator (`async for`) | Async generator (`for await`) | Async loop (`while let`) |
| Sync support | ✅ (channels are sync-friendly) | ✅ `SyncClient` | — | — |
| Ack | `consumer.Ack(offset)` | `await consumer.ack(offset)` | `consumer.ack(offset)` | `consumer.ack(offset)` |
| Close | `consumer.Close()` | `await consumer.close()` | `consumer.close()` | `consumer.close()` / drop |
| Auto-reconnect | ✅ | ✅ | ✅ | ✅ |
| SSE support | ✅ | ✅ | ✅ | ✅ |
| WebSocket support | ✅ | ✅ | ✅ | ✅ |

---

## Running all tests

```bash
# Go
cd go && go test ./... -race -v

# Python
cd python && pip install -e ".[dev]" && pytest tests/ -v

# JavaScript
cd javascript && npm install && npm test

# Rust
cd rust && cargo test
```

---

## Broker

The broker that these SDKs connect to is at [github.com/GordenArcher/streamq](https://github.com/GordenArcher/streamq).

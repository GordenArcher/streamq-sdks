# streamq-rs

Official Rust SDK for [streamq](https://github.com/GordenArcher/streamq) — a lightweight message broker with WebSocket and SSE delivery.

## Installation

Add to your `Cargo.toml`:

```toml
[dependencies]
streamq-rs = "0.1"
tokio = { version = "1", features = ["full"] }
```

---

## Quick start

```rust
use streamq::{StreamqClient, ConsumerOptions};

#[tokio::main]
async fn main() -> streamq::Result<()> {
    let client = StreamqClient::new("http://localhost:8080")?;

    // Create a topic
    client.create_topic("payments").await?;

    // Publish a message
    let result = client
        .publish("payments", b"{\"amount\": 100, \"currency\": \"GHS\"}")
        .await?;
    println!("published at offset {}", result.offset);

    // Subscribe and receive messages
    let mut consumer = client.subscribe("payments", ConsumerOptions {
        group: "billing".to_string(),
        from_offset: 0,
        ..Default::default()
    });

    while let Some(msg) = consumer.next().await? {
        let text = String::from_utf8_lossy(&msg.payload);
        println!("offset {}: {text}", msg.offset);
        consumer.ack(msg.offset); // explicit ack for at-least-once delivery
    }

    Ok(())
}
```

---

## API

### `StreamqClient::new(base_url)`

```rust
let client = StreamqClient::new("http://localhost:8080")?;
```

Uses `rustls`, no OpenSSL dependency required. The client is cheap to clone; the underlying connection pool is shared.

### Topic management

```rust
// Create
client.create_topic("events").await?;

// List all topics
let topics = client.list_topics().await?;
for t in &topics {
    println!("{}: {} messages", t.name, t.message_count);
}

// Get single topic stats
let info = client.get_topic("events").await?;
println!("latest offset: {}", info.latest_offset);

// Delete
client.delete_topic("events").await?;
```

### Publishing

```rust
// Payload is &[u8], base64-encoded automatically
let result = client.publish("payments", b"{\"amount\": 100}").await?;
// result: PublishResult { offset, topic, timestamp }
```

### Subscribing

```rust
use streamq::{ConsumerOptions, Protocol};

// Broadcast, receive every message
let mut consumer = client.subscribe("payments", ConsumerOptions::default());

// Consumer group, work queue (one message → one subscriber in group)
let mut consumer = client.subscribe("payments", ConsumerOptions {
    group: "billing".to_string(),
    ..Default::default()
});

// Replay from the beginning of retained history
let mut consumer = client.subscribe("payments", ConsumerOptions {
    from_offset: 0,
    ..Default::default()
});

// SSE instead of WebSocket
let mut consumer = client.subscribe("payments", ConsumerOptions {
    protocol: Protocol::Sse,
    ..Default::default()
});

// Receive messages
while let Some(msg) = consumer.next().await? {
    let text = String::from_utf8_lossy(&msg.payload);
    println!("offset {}: {text}", msg.offset);
    consumer.ack(msg.offset);
}
```

### Error handling

```rust
use streamq::Error;

match client.create_topic("payments").await {
    Ok(_) => {}
    Err(Error::Conflict(msg)) => {
        // topic already exists, that's fine
    }
    Err(Error::NotFound(msg)) => {
        eprintln!("not found: {msg}");
    }
    Err(e) => {
        eprintln!("error: {e}");
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

For at-least-once: use WebSocket with a consumer group and call `consumer.ack(msg.offset)` after processing. On reconnect, the broker redelivers from the last acknowledged offset.

---

## Running tests

```bash
cargo test
```

Tests use `wiremock` for a real in-process HTTP server, no broker required.

---

## Broker

The broker is at [github.com/GordenArcher/streamq](https://github.com/GordenArcher/streamq).

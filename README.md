# streamq-sdks

Official client SDKs for [streamq](https://github.com/GordenArcher/streamq) — a lightweight message broker with WebSocket and SSE delivery.

## SDKs

| Language | Package | Status |
|----------|---------|--------|
| Go | `github.com/GordenArcher/streamq-go` | ✅ Available |
| Python | `streamq-python` (PyPI) | 🔜 Coming soon |
| JavaScript | `streamq-js` (npm) | 🔜 Coming soon |
| Rust | `streamq-rs` (crates.io) | 🔜 Coming soon |

## Go SDK

```bash
go get github.com/GordenArcher/streamq-go
```

### Quick start

```go
package main

import (
    "context"
    "fmt"
    "log"

    streamq "github.com/GordenArcher/streamq-go"
)

func main() {
    client, err := streamq.NewClient("http://localhost:8080")
    if err != nil {
        log.Fatal(err)
    }
    defer client.Close()

    ctx := context.Background()

    // Create a topic
    client.CreateTopic(ctx, "payments")

    // Publish a message
    result, err := client.Publish(ctx, "payments", []byte(`{"amount":100,"currency":"GHS"}`))
    if err != nil {
        log.Fatal(err)
    }
    fmt.Printf("published at offset %d\n", result.Offset)

    // Subscribe — channel-based, auto-reconnects
    consumer, err := client.Subscribe(ctx, "payments",
        streamq.WithGroup("billing"),
        streamq.WithFromOffset(0),
    )
    if err != nil {
        log.Fatal(err)
    }
    defer consumer.Close()

    for msg := range consumer.Messages() {
        fmt.Printf("offset %d: %s\n", msg.Offset, msg.Payload)
        consumer.Ack(msg.Offset) // explicit ack for at-least-once delivery
    }

    if err := consumer.Err(); err != nil {
        log.Printf("consumer stopped: %v", err)
    }
}
```

### Topic management

```go
// Create
err := client.CreateTopic(ctx, "events")

// List all topics
topics, err := client.ListTopics(ctx)

// Get single topic stats
info, err := client.GetTopic(ctx, "events")
fmt.Printf("%s: %d messages, latest offset %d\n", info.Name, info.MessageCount, info.LatestOffset)

// Delete
err = client.DeleteTopic(ctx, "events")
```

### Publishing

```go
// Publish raw bytes — encoding is up to you (JSON, Protobuf, plain text)
result, err := client.Publish(ctx, "payments", []byte(`{"amount":100}`))
fmt.Printf("offset: %d, timestamp: %s\n", result.Offset, result.Timestamp)
```

### Subscribing

```go
// Broadcast — receive every message (no group)
consumer, _ := client.Subscribe(ctx, "payments")

// Consumer group — one message delivered to one subscriber in the group
consumer, _ := client.Subscribe(ctx, "payments",
    streamq.WithGroup("billing"),
)

// Replay from the beginning of retained history
consumer, _ := client.Subscribe(ctx, "payments",
    streamq.WithFromOffset(0),
)

// Resume from a specific offset
consumer, _ := client.Subscribe(ctx, "payments",
    streamq.WithFromOffset(42),
)

// SSE instead of WebSocket (no ack support, works through more proxies)
consumer, _ := client.Subscribe(ctx, "payments",
    streamq.WithProtocol(streamq.ProtocolSSE),
)
```

### Error handling

```go
import "errors"

err := client.CreateTopic(ctx, "payments")

var conflict *streamq.ErrConflict
if errors.As(err, &conflict) {
    // Topic already exists — that's fine
}

var notFound *streamq.ErrNotFound
if errors.As(err, &notFound) {
    // Topic doesn't exist
}
```

### Client options

```go
client, err := streamq.NewClient("http://localhost:8080",
    streamq.WithHTTPTimeout(5 * time.Second),
    streamq.WithReconnectDelay(1 * time.Second),
    streamq.WithMaxReconnectAttempts(10), // 0 = retry forever
)
```

## Delivery guarantees

| Setup | Guarantee | How |
|-------|-----------|-----|
| WebSocket + group + `Ack()` | At-least-once | Broker redelivers from last committed offset on reconnect |
| WebSocket + no group | At-most-once | Auto-committed on send |
| SSE (any) | At-most-once | Unidirectional — no ack channel |

## Running tests

```bash
cd go
go test ./... -race -v
```

Tests use an in-process mock broker — no real streamq instance required.

## Broker

The broker is at [github.com/GordenArcher/streamq](https://github.com/GordenArcher/streamq).

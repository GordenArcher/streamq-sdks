# streamq-go

Official Go SDK for [streamq](https://github.com/GordenArcher/streamq) — a lightweight message broker with WebSocket and SSE delivery.

## Installation

```bash
go get github.com/GordenArcher/streamq-go
```

Requires Go 1.22+.

---

## Quick start

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
    if err := client.CreateTopic(ctx, "payments"); err != nil {
        log.Fatal(err)
    }

    // Publish a message
    result, err := client.Publish(ctx, "payments", []byte(`{"amount":100,"currency":"GHS"}`))
    if err != nil {
        log.Fatal(err)
    }
    fmt.Printf("published at offset %d\n", result.Offset)

    // Subscribe and receive messages
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

---

## API

### `NewClient(baseURL, ...ClientOption)`

```go
client, err := streamq.NewClient("http://localhost:8080",
    streamq.WithHTTPTimeout(5 * time.Second),
    streamq.WithReconnectDelay(1 * time.Second),
    streamq.WithMaxReconnectAttempts(10), // 0 = retry forever (default)
)
defer client.Close()
```

### Topic management

```go
// Create
err := client.CreateTopic(ctx, "events")

// List all topics
topics, err := client.ListTopics(ctx)
for _, t := range topics {
    fmt.Printf("%s: %d messages, latest offset %d\n",
        t.Name, t.MessageCount, t.LatestOffset)
}

// Get single topic stats
info, err := client.GetTopic(ctx, "events")

// Delete
err = client.DeleteTopic(ctx, "events")
```

### Publishing

```go
// Payload is []byte — encoding is up to you (JSON, Protobuf, plain text)
result, err := client.Publish(ctx, "payments", []byte(`{"amount":100}`))
fmt.Printf("offset: %d, timestamp: %s\n", result.Offset, result.Timestamp)
```

### Subscribing

```go
// Broadcast — receive every message (no group)
consumer, err := client.Subscribe(ctx, "payments")

// Consumer group — work queue (one message → one subscriber in group)
consumer, err := client.Subscribe(ctx, "payments",
    streamq.WithGroup("billing"),
)

// Replay from the beginning of retained history
consumer, err := client.Subscribe(ctx, "payments",
    streamq.WithFromOffset(0),
)

// Resume from a specific offset
consumer, err := client.Subscribe(ctx, "payments",
    streamq.WithFromOffset(42),
)

// SSE instead of WebSocket (no ack support, works through more proxies)
consumer, err := client.Subscribe(ctx, "payments",
    streamq.WithProtocol(streamq.ProtocolSSE),
)

// Iterate — channel-based, composable with select
for msg := range consumer.Messages() {
    process(msg)
    consumer.Ack(msg.Offset)
}

// Works naturally with select for timeout or cancellation
for {
    select {
    case msg, ok := <-consumer.Messages():
        if !ok {
            return // consumer stopped
        }
        process(msg)
        consumer.Ack(msg.Offset)
    case <-ctx.Done():
        consumer.Close()
        return
    }
}
```

### Error handling

```go
import "errors"

err := client.CreateTopic(ctx, "payments")

var conflict *streamq.ErrConflict
if errors.As(err, &conflict) {
    // topic already exists, that's fine, continue
}

var notFound *streamq.ErrNotFound
if errors.As(err, &notFound) {
    // topic doesn't exist
}

// Check why a consumer stopped
if err := consumer.Err(); err != nil {
    var maxReconnects *streamq.ErrMaxReconnects
    if errors.As(err, &maxReconnects) {
        fmt.Printf("gave up after %d attempts\n", maxReconnects.Attempts)
    }
}
```

---

## Delivery guarantees

| Setup | Guarantee | Notes |
|-------|-----------|-------|
| WebSocket + group + `Ack()` | At-least-once | Broker redelivers from last committed offset on reconnect |
| WebSocket, no group | At-most-once | Auto-committed on send |
| SSE (any) | At-most-once | Unidirectional — no ack channel |

For at-least-once delivery: use WebSocket with a consumer group and call `consumer.Ack(msg.Offset)` after successfully processing each message. On reconnect, the broker redelivers from the last acknowledged offset.

---

## Consumer lifecycle

```go
consumer, err := client.Subscribe(ctx, "payments", streamq.WithGroup("billing"))
if err != nil {
    log.Fatal(err)
}

// Messages() is closed when the consumer stops.
// Range over it, exits cleanly on close or fatal error.
for msg := range consumer.Messages() {
    process(msg)
    consumer.Ack(msg.Offset)
}

// Always check Err() after the loop.
// nil  → consumer was closed cleanly via Close() or context cancellation.
// err  → connection failed permanently (ErrMaxReconnects or similar).
if err := consumer.Err(); err != nil {
    log.Printf("consumer error: %v", err)
}
```

The consumer reconnects automatically on connection drops. Each reconnect resumes from the last committed offset for grouped consumers, so no messages are lost or redelivered across reconnects (assuming acks were sent).

---

## Running tests

```bash
cd go
go test ./... -race -v
```

Tests use an in-process `httptest.Server` mock, no real broker required.

---

## Broker

The broker is at [github.com/GordenArcher/streamq](https://github.com/GordenArcher/streamq).

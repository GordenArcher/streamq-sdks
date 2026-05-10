// Package streamq provides the official Go SDK for streamq, a lightweight
// message broker with HTTP publishing and WebSocket or Server-Sent Events
// delivery.
//
// The SDK is centered around Client. A Client manages topic metadata through
// the broker HTTP API, publishes byte payloads to topics, and creates Consumer
// instances for long-lived subscriptions.
//
// Install the module with:
//
//	go get github.com/GordenArcher/streamq-sdks/go
//
// Then import it in your application:
//
//	import streamq "github.com/GordenArcher/streamq-sdks/go"
//
// # Client Setup
//
// Create a client with the broker base URL. The client strips a trailing slash
// from the URL and owns the HTTP client used for management and publish
// requests.
//
//	client, err := streamq.NewClient("http://localhost:8080",
//		streamq.WithHTTPTimeout(5*time.Second),
//		streamq.WithReconnectDelay(time.Second),
//		streamq.WithMaxReconnectAttempts(10),
//	)
//	if err != nil {
//		log.Fatal(err)
//	}
//	defer client.Close()
//
// WithMaxReconnectAttempts controls subscription reconnect behavior for
// consumers created by the client. A value of 0 means retry forever.
//
// # Topic Management
//
// Topics can be created, listed, inspected, and deleted with context-aware
// methods. Broker errors are returned as typed Go errors so callers can use
// errors.As for control flow.
//
//	ctx := context.Background()
//
//	if err := client.CreateTopic(ctx, "payments"); err != nil {
//		var conflict *streamq.ErrConflict
//		if !errors.As(err, &conflict) {
//			log.Fatal(err)
//		}
//	}
//
//	topics, err := client.ListTopics(ctx)
//	if err != nil {
//		log.Fatal(err)
//	}
//
//	for _, topic := range topics {
//		fmt.Printf("%s has %d messages\n", topic.Name, topic.MessageCount)
//	}
//
// # Publishing
//
// Publish accepts a topic name and a []byte payload. The SDK does not impose a
// serialization format; applications can publish JSON, text, protobuf, or any
// other byte encoding understood by their consumers.
//
//	result, err := client.Publish(ctx, "payments", []byte(`{"amount":100,"currency":"GHS"}`))
//	if err != nil {
//		log.Fatal(err)
//	}
//
//	fmt.Printf("published at offset %d\n", result.Offset)
//
// # Subscribing
//
// Subscribe returns a Consumer. Messages are delivered through the channel
// returned by Consumer.Messages. The channel closes when the consumer is
// closed, the context is cancelled, or reconnect attempts are exhausted.
//
//	consumer, err := client.Subscribe(ctx, "payments",
//		streamq.WithGroup("billing"),
//		streamq.WithFromOffset(0),
//		streamq.WithBufferSize(128),
//	)
//	if err != nil {
//		log.Fatal(err)
//	}
//	defer consumer.Close()
//
//	for msg := range consumer.Messages() {
//		if err := process(msg); err != nil {
//			log.Printf("process offset %d: %v", msg.Offset, err)
//			continue
//		}
//
//		if err := consumer.Ack(msg.Offset); err != nil {
//			log.Printf("ack offset %d: %v", msg.Offset, err)
//		}
//	}
//
//	if err := consumer.Err(); err != nil {
//		log.Printf("consumer stopped: %v", err)
//	}
//
// A subscription without WithGroup receives every message published to the
// topic. A subscription with WithGroup participates in a consumer group, where
// each message is delivered to one member of the group.
//
// WithFromOffset controls replay. Use -1 to start at the tail, 0 to replay
// retained history from the beginning, or a specific offset to resume from
// that point.
//
// # Delivery Guarantees
//
// The default transport is WebSocket. WebSocket subscriptions support explicit
// acknowledgements through Consumer.Ack. When a consumer group is used, calling
// Ack after successful processing enables at-least-once delivery across
// reconnects.
//
// SSE can be selected when a unidirectional HTTP stream is preferred:
//
//	consumer, err := client.Subscribe(ctx, "payments",
//		streamq.WithProtocol(streamq.ProtocolSSE),
//	)
//
// SSE subscriptions do not support acknowledgements and provide at-most-once
// delivery. They are useful in environments where a simple server-to-client
// stream is easier to proxy than WebSocket.
//
// # Error Handling
//
// API errors are exposed as typed errors. This lets applications distinguish
// expected states such as conflicts and missing topics from unexpected
// transport or server failures.
//
//	err := client.DeleteTopic(ctx, "missing")
//
//	var notFound *streamq.ErrNotFound
//	if errors.As(err, &notFound) {
//		return
//	}
//	if err != nil {
//		log.Fatal(err)
//	}
//
// For consumers, check Consumer.Err after the message channel closes:
//
//	for range consumer.Messages() {
//		// process messages
//	}
//
//	var maxReconnects *streamq.ErrMaxReconnects
//	if errors.As(consumer.Err(), &maxReconnects) {
//		log.Printf("gave up after %d reconnect attempts", maxReconnects.Attempts)
//	}
package streamq

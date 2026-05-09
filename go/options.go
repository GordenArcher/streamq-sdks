// Option types for configuring the client and subscriptions.
//
// We use the functional options pattern (WithX functions that return an
// OptionFunc) rather than a config struct for two reasons:
//
//   1. Forward compatibility: adding a new option never breaks existing
//      callers. A new WithX function is additive, callers that don't use
//      it are unaffected.
//
//   2. Readable call sites:
//        client.Subscribe("payments", streamq.WithGroup("billing"), streamq.WithFromOffset(0))
//      reads more clearly than constructing a struct literal with zero values
//      for every unused field.
//
// This is the standard Go option pattern used by the official Google Cloud
// libraries, gRPC, and most well-maintained Go SDKs.

package streamq

import "time"

// ClientOptions holds configuration for the top-level Client.
type ClientOptions struct {
	// HTTPTimeout is the timeout for individual HTTP API calls (create topic,
	// publish, list topics, etc.). Does not apply to long-lived streaming
	// connections. Default: 10 seconds.
	HTTPTimeout time.Duration

	// ReconnectDelay is the time to wait before attempting to reconnect a
	// dropped WebSocket or SSE connection. Default: 2 seconds.
	ReconnectDelay time.Duration

	// MaxReconnectAttempts is the maximum number of reconnect attempts before
	// the consumer gives up and closes its Messages() channel with an error.
	// 0 means retry indefinitely. Default: 0.
	MaxReconnectAttempts int
}

// defaultClientOptions returns sensible defaults.
func defaultClientOptions() ClientOptions {
	return ClientOptions{
		HTTPTimeout:          10 * time.Second,
		ReconnectDelay:       2 * time.Second,
		MaxReconnectAttempts: 0, // retry forever by default
	}
}

// ClientOption is a function that mutates a ClientOptions struct.
// Callers pass these to NewClient.
type ClientOption func(*ClientOptions)

// WithHTTPTimeout sets the timeout for HTTP API calls.
func WithHTTPTimeout(d time.Duration) ClientOption {
	return func(o *ClientOptions) {
		o.HTTPTimeout = d
	}
}

// WithReconnectDelay sets how long to wait between reconnect attempts.
func WithReconnectDelay(d time.Duration) ClientOption {
	return func(o *ClientOptions) {
		o.ReconnectDelay = d
	}
}

// WithMaxReconnectAttempts sets the maximum reconnect attempts.
// Pass 0 for unlimited retries.
func WithMaxReconnectAttempts(n int) ClientOption {
	return func(o *ClientOptions) {
		o.MaxReconnectAttempts = n
	}
}

// SubscribeOptions holds configuration for a single subscription.
type SubscribeOptions struct {
	// Group is the consumer group ID. When set, only one subscriber in the
	// group receives each message (competing consumers / work queue pattern).
	// When empty, this subscriber receives every message (broadcast / fan-out).
	Group string

	// FromOffset is the absolute offset to start reading from.
	// -1 means "start from the tail" (only new messages, no history replay).
	//  0 means "start from the beginning of retained history".
	//  N means "start from offset N".
	// Default: -1 (tail).
	FromOffset int64

	// Protocol selects the underlying transport.
	// ProtocolWebSocket provides bidirectional communication and supports
	// explicit acks (at-least-once delivery for grouped consumers).
	// ProtocolSSE is simpler, works through more proxies, but is
	// unidirectional (at-most-once delivery).
	// Default: ProtocolWebSocket.
	Protocol Protocol

	// BufferSize is the number of messages the SDK buffers internally
	// before the caller's read loop needs to drain them. A larger buffer
	// absorbs bursts without dropping. Default: 64.
	BufferSize int
}

// Protocol selects the streaming transport for a subscription.
type Protocol int

const (
	// ProtocolWebSocket uses WebSocket for bidirectional streaming.
	// Supports explicit acks for at-least-once delivery.
	ProtocolWebSocket Protocol = iota

	// ProtocolSSE uses Server-Sent Events for server-to-client streaming.
	// Simpler, works through more HTTP proxies, but no ack support.
	ProtocolSSE
)

// defaultSubscribeOptions returns sensible defaults for Subscribe.
func defaultSubscribeOptions() SubscribeOptions {
	return SubscribeOptions{
		FromOffset: -1, // tail — only new messages
		Protocol:   ProtocolWebSocket,
		BufferSize: 64,
	}
}

// SubscribeOption is a function that mutates a SubscribeOptions struct.
type SubscribeOption func(*SubscribeOptions)

// WithGroup sets the consumer group for this subscription.
func WithGroup(group string) SubscribeOption {
	return func(o *SubscribeOptions) {
		o.Group = group
	}
}

// WithFromOffset sets the starting offset for replay.
// Pass 0 to get all retained history, or a specific offset to resume.
func WithFromOffset(offset int64) SubscribeOption {
	return func(o *SubscribeOptions) {
		o.FromOffset = offset
	}
}

// WithProtocol selects the streaming transport (WebSocket or SSE).
func WithProtocol(p Protocol) SubscribeOption {
	return func(o *SubscribeOptions) {
		o.Protocol = p
	}
}

// WithBufferSize sets the internal message buffer size for this consumer.
func WithBufferSize(n int) SubscribeOption {
	return func(o *SubscribeOptions) {
		o.BufferSize = n
	}
}

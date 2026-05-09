// Shared types used across the SDK.
//
// Message mirrors the JSON structure the broker sends over both WebSocket
// and SSE connections. Payload is []byte which Go's JSON decoder
// automatically base64-decodes, callers receive raw bytes and can
// interpret them however they like (JSON, Protobuf, plain text).

package streamq

import "time"

// Message is a single event received from a streamq topic.
type Message struct {
	// ID is the broker-assigned unique identifier for this message.
	ID string `json:"id"`

	// Offset is the zero-based absolute position of this message in the
	// topic log. Use this value to Ack() a WebSocket consumer, or pass it
	// as FromOffset when subscribing to resume from a specific point.
	Offset int64 `json:"offset"`

	// Topic is the name of the topic this message was published to.
	Topic string `json:"topic"`

	// Payload is the raw message body. The broker treats it as opaque bytes —
	// the SDK decodes from base64 automatically via json.Unmarshal.
	Payload []byte `json:"payload"`

	// Timestamp is the server-side time the broker received the message.
	// Set by the broker, not the producer, so it's reliable regardless of
	// client clock skew.
	Timestamp time.Time `json:"timestamp"`
}

// ackFrame is the JSON structure sent to the broker to acknowledge a message.
// Only used by WebSocket consumers, SSE is unidirectional.
type ackFrame struct {
	Ack int64 `json:"ack"`
}

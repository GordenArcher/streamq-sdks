// Consumer handles a long-lived subscription to a streamq topic.
//
// Design:
// The consumer exposes a single read channel (Messages()) and manages all
// connection lifecycle internally, initial connect, reconnect on failure,
// clean shutdown. The caller's code is a simple range loop:
//
//   for msg := range consumer.Messages() {
//       process(msg)
//       consumer.Ack(msg.Offset) // for WebSocket + group consumers
//   }
//   if err := consumer.Err(); err != nil {
//       // connection failed permanently
//   }
//
// Reconnect behaviour:
// If the broker connection drops (network error, broker restart), the consumer
// waits ReconnectDelay and tries again. If the subscription had a group and
// a committed offset, the broker will resume from the committed offset
// automatically, no messages are redelivered. If MaxReconnectAttempts is
// set and exhausted, Messages() is closed and Err() returns ErrMaxReconnects.
//
// At-least-once delivery (WebSocket + group):
// The consumer does NOT auto-ack. Call Ack(offset) after successfully
// processing a message. Until acked, the broker considers the message
// undelivered and will redeliver from the last committed offset on reconnect.
//
// At-most-once delivery (SSE or ungrouped):
// No ack mechanism, the broker auto-commits on send. If the consumer
// crashes after receiving but before processing, the message is lost.
//
// Thread safety:
// Messages() and Err() are safe to call from any goroutine.
// Ack() is safe to call from any goroutine.
// Close() is safe to call from any goroutine and is idempotent.

package streamq

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

// Consumer is a long-lived subscription to a streamq topic.
// Obtain one via client.Subscribe().
type Consumer struct {
	client *Client
	topic  string
	opts   SubscribeOptions
	ctx    context.Context
	cancel context.CancelFunc

	// msgs is the channel the caller reads from.
	// Closed when the consumer stops (either via Close() or fatal error).
	msgs chan Message

	// ackCh carries ack offsets from Ack() to the write goroutine (WS only).
	// Buffered so Ack() never blocks the caller.
	ackCh chan int64

	// err holds the terminal error (if any) after msgs is closed.
	// Protected by errMu.
	err   error
	errMu sync.RWMutex

	// closeOnce ensures Close() is idempotent.
	closeOnce sync.Once
}

// newConsumer creates a Consumer but does not start the connection.
// Call start() to begin.
func newConsumer(ctx context.Context, client *Client, topic string, opts SubscribeOptions) *Consumer {
	ctx, cancel := context.WithCancel(ctx)
	return &Consumer{
		client: client,
		topic:  topic,
		opts:   opts,
		ctx:    ctx,
		cancel: cancel,
		msgs:   make(chan Message, opts.BufferSize),
		ackCh:  make(chan int64, opts.BufferSize),
	}
}

// start launches the background goroutine that manages the connection.
func (c *Consumer) start() {
	go c.run()
}

// Messages returns the channel of incoming messages.
// The channel is closed when the consumer stops.
// Range over this channel:
//
//	for msg := range consumer.Messages() { ... }
func (c *Consumer) Messages() <-chan Message {
	return c.msgs
}

// Ack sends an acknowledgement for the given offset to the broker.
// Only meaningful for WebSocket consumers in a consumer group —
// the broker advances the group's committed offset on receipt.
//
// For SSE consumers or ungrouped consumers, Ack is a no-op.
// Safe to call from any goroutine.
func (c *Consumer) Ack(offset int64) {
	if c.opts.Protocol == ProtocolSSE || c.opts.Group == "" {
		return
	}
	select {
	case c.ackCh <- offset:
	default:
		// ack channel full, this shouldn't happen under normal operation
		// (caller is acking faster than the write goroutine can send).
		// Drop silently; the worst case is the broker redelivers on reconnect.
	}
}

// Err returns the terminal error that caused the consumer to stop.
// Returns nil if the consumer was closed cleanly via Close(), or if it
// is still running.
// Call after the Messages() channel is closed to distinguish clean
// shutdown from failure.
func (c *Consumer) Err() error {
	c.errMu.RLock()
	defer c.errMu.RUnlock()
	return c.err
}

// Close stops the consumer and closes the Messages() channel.
// Idempotent, safe to call multiple times.
func (c *Consumer) Close() {
	c.closeOnce.Do(func() {
		c.cancel()
	})
}

// run is the main consumer loop. It manages reconnects and delegates to
// the protocol-specific connect function.
func (c *Consumer) run() {
	defer close(c.msgs)

	attempts := 0
	for {
		// Check if we've been closed before attempting a connection.
		select {
		case <-c.ctx.Done():
			// Clean close, set ErrConsumerClosed so callers know it was intentional.
			c.setErr(&ErrConsumerClosed{})
			return
		default:
		}

		// Connect using the selected protocol.
		var connErr error
		switch c.opts.Protocol {
		case ProtocolSSE:
			connErr = c.connectSSE()
		default:
			connErr = c.connectWebSocket()
		}

		// nil error means the context was cancelled (clean close).
		if connErr == nil {
			c.setErr(&ErrConsumerClosed{})
			return
		}

		// Context cancelled during connection, clean shutdown.
		if c.ctx.Err() != nil {
			c.setErr(&ErrConsumerClosed{})
			return
		}

		attempts++

		// Check reconnect limit.
		if c.client.opts.MaxReconnectAttempts > 0 && attempts >= c.client.opts.MaxReconnectAttempts {
			c.setErr(&ErrMaxReconnects{Attempts: attempts, Last: connErr})
			return
		}

		// Wait before reconnecting. Respect context cancellation during wait.
		select {
		case <-time.After(c.client.opts.ReconnectDelay):
		case <-c.ctx.Done():
			c.setErr(&ErrConsumerClosed{})
			return
		}
	}
}

// connectWebSocket establishes a WebSocket connection and pumps messages
// until the connection drops or the context is cancelled.
// Returns nil on clean shutdown, error on connection failure.
func (c *Consumer) connectWebSocket() error {
	wsURL := c.buildURL("ws")

	conn, _, err := websocket.DefaultDialer.DialContext(c.ctx, wsURL, nil)
	if err != nil {
		return fmt.Errorf("ws dial: %w", err)
	}
	defer conn.Close()

	// done is closed when the read pump exits, signalling the write pump to stop.
	done := make(chan struct{})

	// Write pump: sends ack frames to the broker.
	// Runs in a separate goroutine, gorilla/websocket requires exactly one
	// concurrent writer and one concurrent reader.
	go func() {
		defer conn.Close()
		for {
			select {
			case offset := <-c.ackCh:
				frame := ackFrame{Ack: offset}
				data, err := json.Marshal(frame)
				if err != nil {
					continue
				}
				if err := conn.WriteMessage(websocket.TextMessage, data); err != nil {
					return
				}
			case <-done:
				conn.WriteMessage(
					websocket.CloseMessage,
					websocket.FormatCloseMessage(websocket.CloseNormalClosure, ""),
				)
				return
			case <-c.ctx.Done():
				conn.WriteMessage(
					websocket.CloseMessage,
					websocket.FormatCloseMessage(websocket.CloseNormalClosure, ""),
				)
				return
			}
		}
	}()

	// Read pump: receives message frames from the broker.
	defer close(done)
	for {
		_, data, err := conn.ReadMessage()
		if err != nil {
			// Distinguish clean close from network error.
			if websocket.IsCloseError(err, websocket.CloseNormalClosure, websocket.CloseGoingAway) {
				return nil
			}
			if c.ctx.Err() != nil {
				return nil
			}
			return fmt.Errorf("ws read: %w", err)
		}

		var msg Message
		if err := json.Unmarshal(data, &msg); err != nil {
			// Malformed frame, skip.
			continue
		}

		// Send to caller's channel. Respect context cancellation.
		select {
		case c.msgs <- msg:
		case <-c.ctx.Done():
			return nil
		}
	}
}

// connectSSE establishes an SSE connection and pumps events until the
// connection drops or the context is cancelled.
// Returns nil on clean shutdown, error on connection failure.
func (c *Consumer) connectSSE() error {
	sseURL := c.buildURL("sse")

	req, err := http.NewRequestWithContext(c.ctx, http.MethodGet, sseURL, nil)
	if err != nil {
		return fmt.Errorf("sse: build request: %w", err)
	}
	req.Header.Set("Accept", "text/event-stream")
	req.Header.Set("Cache-Control", "no-cache")

	// Use a client without a timeout for the SSE connection —
	// the base client's Timeout applies to the entire response body,
	// which would terminate the stream prematurely.
	httpClient := &http.Client{}
	resp, err := httpClient.Do(req)
	if err != nil {
		if c.ctx.Err() != nil {
			return nil
		}
		return fmt.Errorf("sse: connect: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("sse: unexpected status %d", resp.StatusCode)
	}

	// Parse SSE events line by line.
	// SSE format:
	//   id: <offset>
	//   data: <json>
	//   <blank line>  ← event boundary
	scanner := bufio.NewScanner(resp.Body)
	var dataLine string

	for scanner.Scan() {
		line := scanner.Text()

		if strings.HasPrefix(line, "data: ") {
			dataLine = strings.TrimPrefix(line, "data: ")
			continue
		}

		// Blank line = end of event. Process if we have data.
		if line == "" && dataLine != "" {
			var msg Message
			if err := json.Unmarshal([]byte(dataLine), &msg); err != nil {
				dataLine = ""
				continue
			}

			select {
			case c.msgs <- msg:
			case <-c.ctx.Done():
				return nil
			}

			dataLine = ""
		}
	}

	if err := scanner.Err(); err != nil {
		if c.ctx.Err() != nil {
			return nil
		}
		return fmt.Errorf("sse: read: %w", err)
	}

	return nil
}

// buildURL constructs the subscription URL for the given protocol ("ws" or "sse").
//
// WebSocket:  ws://host/topics/<name>/subscribe/ws?group=X&from_offset=Y
// SSE:        http://host/topics/<name>/subscribe/sse?group=X&from_offset=Y
func (c *Consumer) buildURL(protocol string) string {
	base := c.client.baseURL

	var scheme string
	switch protocol {
	case "ws":
		// Replace http(s) with ws(s) for WebSocket URLs.
		scheme = strings.Replace(base, "https://", "wss://", 1)
		scheme = strings.Replace(scheme, "http://", "ws://", 1)
	default:
		scheme = base
	}

	path := fmt.Sprintf("%s/topics/%s/subscribe/%s", scheme, c.topic, protocol)

	params := url.Values{}
	if c.opts.Group != "" {
		params.Set("group", c.opts.Group)
	}
	if c.opts.FromOffset >= 0 {
		params.Set("from_offset", strconv.FormatInt(c.opts.FromOffset, 10))
	}

	if len(params) > 0 {
		return path + "?" + params.Encode()
	}
	return path
}

// setErr stores the terminal error.
// Only the first call has effect, errors don't overwrite each other.
func (c *Consumer) setErr(err error) {
	c.errMu.Lock()
	defer c.errMu.Unlock()
	if c.err == nil {
		c.err = err
	}
}

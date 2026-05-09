// Client is the main entry point for the streamq Go SDK.
//
// It wraps the broker's HTTP API for topic management and publishing,
// and provides a Subscribe method that returns a Consumer for streaming.
//
// Usage:
//
//   client, err := streamq.NewClient("http://localhost:8080")
//   if err != nil {
//       log.Fatal(err)
//   }
//   defer client.Close()
//
//   // Create a topic
//   err = client.CreateTopic(ctx, "payments")
//
//   // Publish a message
//   offset, err := client.Publish(ctx, "payments", []byte(`{"amount":100}`))
//
//   // Subscribe
//   consumer, err := client.Subscribe(ctx, "payments",
//       streamq.WithGroup("billing"),
//       streamq.WithFromOffset(0),
//   )
//   for msg := range consumer.Messages() {
//       process(msg)
//       consumer.Ack(msg.Offset)
//   }
//
// Thread safety:
// Client is safe for concurrent use. All HTTP calls use an internal
// http.Client with connection pooling. Subscribe can be called from
// multiple goroutines simultaneously.

package streamq

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"
)

// Client is the top-level SDK handle. Create one per broker address.
type Client struct {
	baseURL    string
	httpClient *http.Client
	opts       ClientOptions
}

// NewClient creates a Client connected to the broker at baseURL.
// baseURL should be the scheme + host + port: "http://localhost:8080".
// Trailing slashes are stripped automatically.
//
// Options are applied in order, later options override earlier ones.
func NewClient(baseURL string, opts ...ClientOption) (*Client, error) {
	if baseURL == "" {
		return nil, fmt.Errorf("streamq: baseURL cannot be empty")
	}

	// Normalise: strip trailing slash so URL construction is consistent.
	baseURL = strings.TrimRight(baseURL, "/")

	options := defaultClientOptions()
	for _, opt := range opts {
		opt(&options)
	}

	return &Client{
		baseURL: baseURL,
		httpClient: &http.Client{
			// Timeout only applies to non-streaming requests. Long-lived
			// SSE/WS connections are managed by the Consumer, not this client.
			Timeout: options.HTTPTimeout,
		},
		opts: options,
	}, nil
}

// Close releases resources held by the client (idle HTTP connections).
// After Close, the client must not be used.
func (c *Client) Close() {
	c.httpClient.CloseIdleConnections()
}

// CreateTopic creates a new topic on the broker.
// Returns ErrTopicAlreadyExists if the topic already exists.
func (c *Client) CreateTopic(ctx context.Context, name string) error {
	body := map[string]string{"name": name}
	_, err := c.doJSON(ctx, http.MethodPost, "/topics", body, http.StatusCreated)
	return err
}

// DeleteTopic deletes a topic and disconnects all its subscribers.
// Returns ErrTopicNotFound if the topic does not exist.
func (c *Client) DeleteTopic(ctx context.Context, name string) error {
	_, err := c.doJSON(ctx, http.MethodDelete, "/topics/"+name, nil, http.StatusOK)
	return err
}

// TopicInfo holds stats for a single topic returned by ListTopics and GetTopic.
type TopicInfo struct {
	Name            string `json:"name"`
	MessageCount    int    `json:"message_count"`
	SubscriberCount int    `json:"subscriber_count"`
	LatestOffset    int64  `json:"latest_offset"`
}

// ListTopics returns stats for all topics on the broker.
// Returns an empty slice (never nil) if no topics exist.
func (c *Client) ListTopics(ctx context.Context) ([]TopicInfo, error) {
	resp, err := c.doJSON(ctx, http.MethodGet, "/topics", nil, http.StatusOK)
	if err != nil {
		return nil, err
	}

	// The broker wraps responses in an envelope: {"status":"success","data":[...]}
	var envelope struct {
		Data []TopicInfo `json:"data"`
	}
	if err := json.Unmarshal(resp, &envelope); err != nil {
		return nil, fmt.Errorf("streamq: decode topic list: %w", err)
	}

	if envelope.Data == nil {
		return []TopicInfo{}, nil
	}
	return envelope.Data, nil
}

// GetTopic returns stats for a single topic.
// Returns ErrTopicNotFound if the topic does not exist.
func (c *Client) GetTopic(ctx context.Context, name string) (TopicInfo, error) {
	resp, err := c.doJSON(ctx, http.MethodGet, "/topics/"+name, nil, http.StatusOK)
	if err != nil {
		return TopicInfo{}, err
	}

	var envelope struct {
		Data TopicInfo `json:"data"`
	}
	if err := json.Unmarshal(resp, &envelope); err != nil {
		return TopicInfo{}, fmt.Errorf("streamq: decode topic info: %w", err)
	}
	return envelope.Data, nil
}

// PublishResult holds the broker's response to a publish request.
type PublishResult struct {
	Offset    int64     `json:"offset"`
	Topic     string    `json:"topic"`
	Timestamp time.Time `json:"timestamp"`
}

// Publish sends payload to the named topic.
// payload is sent as-is (base64-encoded in the JSON request body).
// Returns the assigned offset and server timestamp on success.
//
// Publish is synchronous, it waits for the broker to confirm the message
// was appended to the log. Fan-out to subscribers happens asynchronously
// in the broker after this call returns.
func (c *Client) Publish(ctx context.Context, topic string, payload []byte) (PublishResult, error) {
	// The broker expects payload as a base64 string in JSON.
	// Go's json.Marshal automatically base64-encodes []byte fields.
	body := map[string][]byte{"payload": payload}

	resp, err := c.doJSON(ctx, http.MethodPost, "/topics/"+topic+"/publish", body, http.StatusAccepted)
	if err != nil {
		return PublishResult{}, err
	}

	var envelope struct {
		Data PublishResult `json:"data"`
	}
	if err := json.Unmarshal(resp, &envelope); err != nil {
		return PublishResult{}, fmt.Errorf("streamq: decode publish result: %w", err)
	}
	return envelope.Data, nil
}

// Subscribe creates a new Consumer for the given topic.
// The consumer starts receiving messages immediately.
// Call consumer.Messages() to get the channel, consumer.Ack() to acknowledge,
// and consumer.Close() when done.
//
// Subscribe is non-blocking, the connection is established in the background.
// If the connection fails, errors are surfaced via consumer.Err().
func (c *Client) Subscribe(ctx context.Context, topic string, opts ...SubscribeOption) (*Consumer, error) {
	options := defaultSubscribeOptions()
	for _, opt := range opts {
		opt(&options)
	}

	consumer := newConsumer(ctx, c, topic, options)
	consumer.start()
	return consumer, nil
}

// doJSON makes an HTTP request, checks the status code, and returns the
// raw response body for the caller to decode.
//
// Why return raw bytes instead of decoding here?
// Different endpoints have different response shapes. Returning raw bytes
// lets each method decode exactly what it needs without a generic interface{}.
func (c *Client) doJSON(ctx context.Context, method, path string, body interface{}, expectStatus int) ([]byte, error) {
	var reqBody io.Reader

	if body != nil {
		data, err := json.Marshal(body)
		if err != nil {
			return nil, fmt.Errorf("streamq: marshal request body: %w", err)
		}
		reqBody = bytes.NewReader(data)
	}

	req, err := http.NewRequestWithContext(ctx, method, c.baseURL+path, reqBody)
	if err != nil {
		return nil, fmt.Errorf("streamq: build request: %w", err)
	}

	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("streamq: %s %s: %w", method, path, err)
	}
	defer resp.Body.Close()

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("streamq: read response body: %w", err)
	}

	if resp.StatusCode != expectStatus {
		return nil, parseAPIError(resp.StatusCode, respBody)
	}

	return respBody, nil
}

// parseAPIError extracts the error message from the broker's JSON envelope
// and wraps it in an SDK error type for the caller to inspect.
func parseAPIError(statusCode int, body []byte) error {
	var envelope struct {
		Message string `json:"message"`
	}
	// Best-effort decode, if the body isn't JSON, use status code only.
	json.Unmarshal(body, &envelope)

	msg := envelope.Message
	if msg == "" {
		msg = fmt.Sprintf("HTTP %d", statusCode)
	}

	switch statusCode {
	case http.StatusNotFound:
		return &ErrNotFound{Message: msg}
	case http.StatusConflict:
		return &ErrConflict{Message: msg}
	default:
		return &ErrAPI{StatusCode: statusCode, Message: msg}
	}
}

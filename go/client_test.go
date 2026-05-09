// Tests for the streamq Go SDK.
//
// Testing strategy:
// We spin up a lightweight httptest.Server that mimics the broker's API
// for each test. This means tests run without a real broker, fast, isolated,
// no external dependencies.
//
// The mock server returns responses that match the broker's exact JSON
// envelope format so the SDK's parsing code is exercised as-is.
//
// For Consumer tests (WebSocket + SSE), the mock server sends a controlled
// sequence of messages then closes the connection, allowing us to assert
// exactly what the consumer received.

package streamq

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

// mockBroker is a thin httptest.Server wrapper with helpers for registering
// handler functions per method+path.
type mockBroker struct {
	server *httptest.Server
}

func newMockBroker(t *testing.T, mux *http.ServeMux) *mockBroker {
	t.Helper()
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	return &mockBroker{server: srv}
}

func (m *mockBroker) URL() string { return m.server.URL }

// jsonResponse writes a broker-style JSON envelope response.
func jsonResponse(w http.ResponseWriter, status int, data interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(map[string]interface{}{
		"status": "success",
		"data":   data,
	})
}

// errorResponse writes a broker-style error response.
func errorResponse(w http.ResponseWriter, status int, message string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(map[string]interface{}{
		"status":  "error",
		"message": message,
	})
}

// newTestClient creates a Client pointed at the mock broker.
func newTestClient(t *testing.T, brokerURL string) *Client {
	t.Helper()
	c, err := NewClient(brokerURL, WithHTTPTimeout(5*time.Second))
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	t.Cleanup(c.Close)
	return c
}

func TestNewClient_EmptyURLReturnsError(t *testing.T) {
	_, err := NewClient("")
	if err == nil {
		t.Error("NewClient(\"\") expected error, got nil")
	}
}

func TestNewClient_StripsTrailingSlash(t *testing.T) {
	c, err := NewClient("http://localhost:8080/")
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	if c.baseURL != "http://localhost:8080" {
		t.Errorf("baseURL = %q, want %q", c.baseURL, "http://localhost:8080")
	}
}

func TestCreateTopic_Success(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /topics", func(w http.ResponseWriter, r *http.Request) {
		var body map[string]string
		json.NewDecoder(r.Body).Decode(&body)
		if body["name"] == "" {
			errorResponse(w, http.StatusBadRequest, "name required")
			return
		}
		jsonResponse(w, http.StatusCreated, map[string]string{"name": body["name"]})
	})

	broker := newMockBroker(t, mux)
	client := newTestClient(t, broker.URL())

	err := client.CreateTopic(context.Background(), "payments")
	if err != nil {
		t.Errorf("CreateTopic: unexpected error: %v", err)
	}
}

func TestCreateTopic_ConflictReturnsErrConflict(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /topics", func(w http.ResponseWriter, r *http.Request) {
		errorResponse(w, http.StatusConflict, "topic already exists")
	})

	broker := newMockBroker(t, mux)
	client := newTestClient(t, broker.URL())

	err := client.CreateTopic(context.Background(), "payments")
	var conflict *ErrConflict
	if !errors.As(err, &conflict) {
		t.Errorf("expected *ErrConflict, got %T: %v", err, err)
	}
}

func TestDeleteTopic_Success(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("DELETE /topics/payments", func(w http.ResponseWriter, r *http.Request) {
		jsonResponse(w, http.StatusOK, nil)
	})

	broker := newMockBroker(t, mux)
	client := newTestClient(t, broker.URL())

	if err := client.DeleteTopic(context.Background(), "payments"); err != nil {
		t.Errorf("DeleteTopic: unexpected error: %v", err)
	}
}

func TestDeleteTopic_NotFoundReturnsErrNotFound(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("DELETE /topics/missing", func(w http.ResponseWriter, r *http.Request) {
		errorResponse(w, http.StatusNotFound, "topic not found")
	})

	broker := newMockBroker(t, mux)
	client := newTestClient(t, broker.URL())

	err := client.DeleteTopic(context.Background(), "missing")
	var notFound *ErrNotFound
	if !errors.As(err, &notFound) {
		t.Errorf("expected *ErrNotFound, got %T: %v", err, err)
	}
}

func TestListTopics_ReturnsParsedTopics(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /topics", func(w http.ResponseWriter, r *http.Request) {
		jsonResponse(w, http.StatusOK, []TopicInfo{
			{Name: "payments", MessageCount: 42, LatestOffset: 41},
			{Name: "events", MessageCount: 7, LatestOffset: 6},
		})
	})

	broker := newMockBroker(t, mux)
	client := newTestClient(t, broker.URL())

	topics, err := client.ListTopics(context.Background())
	if err != nil {
		t.Fatalf("ListTopics: %v", err)
	}
	if len(topics) != 2 {
		t.Fatalf("ListTopics len = %d, want 2", len(topics))
	}
	if topics[0].Name != "payments" {
		t.Errorf("topics[0].Name = %q, want payments", topics[0].Name)
	}
	if topics[0].MessageCount != 42 {
		t.Errorf("topics[0].MessageCount = %d, want 42", topics[0].MessageCount)
	}
}

func TestListTopics_EmptyReturnsEmptySlice(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /topics", func(w http.ResponseWriter, r *http.Request) {
		jsonResponse(w, http.StatusOK, []TopicInfo{})
	})

	broker := newMockBroker(t, mux)
	client := newTestClient(t, broker.URL())

	topics, err := client.ListTopics(context.Background())
	if err != nil {
		t.Fatalf("ListTopics: %v", err)
	}
	if topics == nil {
		t.Error("ListTopics returned nil, want empty slice")
	}
	if len(topics) != 0 {
		t.Errorf("ListTopics len = %d, want 0", len(topics))
	}
}

func TestPublish_ReturnsOffsetAndTimestamp(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Second)

	mux := http.NewServeMux()
	mux.HandleFunc("POST /topics/payments/publish", func(w http.ResponseWriter, r *http.Request) {
		jsonResponse(w, http.StatusAccepted, PublishResult{
			Offset:    7,
			Topic:     "payments",
			Timestamp: now,
		})
	})

	broker := newMockBroker(t, mux)
	client := newTestClient(t, broker.URL())

	result, err := client.Publish(context.Background(), "payments", []byte(`{"amount":100}`))
	if err != nil {
		t.Fatalf("Publish: %v", err)
	}
	if result.Offset != 7 {
		t.Errorf("Offset = %d, want 7", result.Offset)
	}
	if result.Topic != "payments" {
		t.Errorf("Topic = %q, want payments", result.Topic)
	}
}

func TestPublish_TopicNotFoundReturnsErrNotFound(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /topics/missing/publish", func(w http.ResponseWriter, r *http.Request) {
		errorResponse(w, http.StatusNotFound, "topic not found")
	})

	broker := newMockBroker(t, mux)
	client := newTestClient(t, broker.URL())

	_, err := client.Publish(context.Background(), "missing", []byte("data"))
	var notFound *ErrNotFound
	if !errors.As(err, &notFound) {
		t.Errorf("expected *ErrNotFound, got %T: %v", err, err)
	}
}

// TestConsumer_SSE_ReceivesMessages starts a mock SSE endpoint that sends
// 3 messages then closes, and verifies the consumer receives all 3.
func TestConsumer_SSE_ReceivesMessages(t *testing.T) {
	messages := []Message{
		{ID: "0", Offset: 0, Topic: "test", Payload: []byte("msg0")},
		{ID: "1", Offset: 1, Topic: "test", Payload: []byte("msg1")},
		{ID: "2", Offset: 2, Topic: "test", Payload: []byte("msg2")},
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/topics/test/subscribe/sse", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		w.Header().Set("Cache-Control", "no-cache")
		flusher := w.(http.Flusher)

		for _, msg := range messages {
			data, _ := json.Marshal(msg)
			fmt.Fprintf(w, "data: %s\n\n", data)
			flusher.Flush()
		}
		// Close the connection, consumer should stop and close Messages().
	})

	broker := newMockBroker(t, mux)
	client := newTestClient(t, broker.URL())

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	consumer, err := client.Subscribe(ctx, "test", WithProtocol(ProtocolSSE), WithMaxReconnectAttempts(1))
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	defer consumer.Close()

	var received []Message
	for msg := range consumer.Messages() {
		received = append(received, msg)
	}

	if len(received) != len(messages) {
		t.Errorf("received %d messages, want %d", len(received), len(messages))
	}
	for i, msg := range received {
		if msg.Offset != int64(i) {
			t.Errorf("received[%d].Offset = %d, want %d", i, msg.Offset, i)
		}
	}
}

// TestConsumer_Close_StopsConsumer verifies that Close() causes Messages()
// to be closed cleanly and Err() returns ErrConsumerClosed.
func TestConsumer_Close_StopsConsumer(t *testing.T) {
	mux := http.NewServeMux()
	// SSE endpoint that hangs open indefinitely.
	mux.HandleFunc("/topics/hang/subscribe/sse", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		w.(http.Flusher).Flush()
		// Block until client disconnects.
		<-r.Context().Done()
	})

	broker := newMockBroker(t, mux)
	client := newTestClient(t, broker.URL())

	ctx := context.Background()
	consumer, _ := client.Subscribe(ctx, "hang", WithProtocol(ProtocolSSE))

	// Close after a short delay.
	go func() {
		time.Sleep(50 * time.Millisecond)
		consumer.Close()
	}()

	// Drain — should unblock when Close() is called.
	for range consumer.Messages() {
	}

	var closed *ErrConsumerClosed
	if !errors.As(consumer.Err(), &closed) {
		t.Errorf("Err() = %v, want *ErrConsumerClosed", consumer.Err())
	}
}

// TestConsumer_MaxReconnects_StopsAfterLimit verifies that a consumer that
// cannot connect stops after MaxReconnectAttempts and sets ErrMaxReconnects.
func TestConsumer_MaxReconnects_StopsAfterLimit(t *testing.T) {
	// Point the client at a URL that refuses connections.
	client, _ := NewClient(
		"http://localhost:19999", // nothing listening here
		WithReconnectDelay(10*time.Millisecond),
		WithMaxReconnectAttempts(3),
	)
	defer client.Close()

	ctx := context.Background()
	consumer, _ := client.Subscribe(ctx, "test",
		WithProtocol(ProtocolSSE),
		WithMaxReconnectAttempts(3),
	)

	// Drain — should stop after 3 failed attempts.
	for range consumer.Messages() {
	}

	var maxReconnects *ErrMaxReconnects
	if !errors.As(consumer.Err(), &maxReconnects) {
		t.Errorf("Err() = %v, want *ErrMaxReconnects", consumer.Err())
	}
	if maxReconnects.Attempts != 3 {
		t.Errorf("Attempts = %d, want 3", maxReconnects.Attempts)
	}
}

func TestWithOptions_OverrideDefaults(t *testing.T) {
	opts := defaultSubscribeOptions()
	WithGroup("billing")(&opts)
	WithFromOffset(42)(&opts)
	WithProtocol(ProtocolSSE)(&opts)
	WithBufferSize(128)(&opts)

	if opts.Group != "billing" {
		t.Errorf("Group = %q, want billing", opts.Group)
	}
	if opts.FromOffset != 42 {
		t.Errorf("FromOffset = %d, want 42", opts.FromOffset)
	}
	if opts.Protocol != ProtocolSSE {
		t.Errorf("Protocol = %v, want ProtocolSSE", opts.Protocol)
	}
	if opts.BufferSize != 128 {
		t.Errorf("BufferSize = %d, want 128", opts.BufferSize)
	}
}

func TestBuildURL_WebSocket(t *testing.T) {
	client, _ := NewClient("http://localhost:8080")
	consumer := newConsumer(context.Background(), client, "payments", SubscribeOptions{
		Group:      "billing",
		FromOffset: 0,
		Protocol:   ProtocolWebSocket,
		BufferSize: 64,
	})

	u := consumer.buildURL("ws")
	if !contains(u, "ws://localhost:8080") {
		t.Errorf("URL scheme wrong: %s", u)
	}
	if !contains(u, "/topics/payments/subscribe/ws") {
		t.Errorf("URL path wrong: %s", u)
	}
	if !contains(u, "group=billing") {
		t.Errorf("URL missing group param: %s", u)
	}
	if !contains(u, "from_offset=0") {
		t.Errorf("URL missing from_offset param: %s", u)
	}
}

func TestBuildURL_SSE_NoGroup(t *testing.T) {
	client, _ := NewClient("http://localhost:8080")
	consumer := newConsumer(context.Background(), client, "events", SubscribeOptions{
		FromOffset: -1, // tail, should not appear in URL
		Protocol:   ProtocolSSE,
		BufferSize: 64,
	})

	u := consumer.buildURL("sse")
	if !contains(u, "/topics/events/subscribe/sse") {
		t.Errorf("URL path wrong: %s", u)
	}
	if contains(u, "from_offset") {
		t.Errorf("URL should not include from_offset for tail: %s", u)
	}
	if contains(u, "group") {
		t.Errorf("URL should not include group when empty: %s", u)
	}
}

func contains(s, sub string) bool {
	return len(s) >= len(sub) && (s == sub || len(s) > 0 && containsStr(s, sub))
}

func containsStr(s, sub string) bool {
	for i := 0; i <= len(s)-len(sub); i++ {
		if s[i:i+len(sub)] == sub {
			return true
		}
	}
	return false
}

// SDK error types.
//
// Why typed errors instead of fmt.Errorf strings?
// Callers can inspect error types with errors.As() to handle specific cases:
//
//   err := client.CreateTopic(ctx, "payments")
//   var conflict *streamq.ErrConflict
//   if errors.As(err, &conflict) {
//       // topic already exists — that's fine, continue
//   }
//
// String comparison on error messages is fragile, a broker message change
// would silently break caller logic. Typed errors are a stable API contract.

package streamq

import "fmt"

// ErrNotFound is returned when the broker responds with 404.
// Typically: topic does not exist.
type ErrNotFound struct {
	Message string
}

func (e *ErrNotFound) Error() string {
	return fmt.Sprintf("streamq: not found: %s", e.Message)
}

// ErrConflict is returned when the broker responds with 409.
// Typically: topic already exists on CreateTopic.
type ErrConflict struct {
	Message string
}

func (e *ErrConflict) Error() string {
	return fmt.Sprintf("streamq: conflict: %s", e.Message)
}

// ErrAPI is returned for any other non-2xx broker response.
type ErrAPI struct {
	StatusCode int
	Message    string
}

func (e *ErrAPI) Error() string {
	return fmt.Sprintf("streamq: API error %d: %s", e.StatusCode, e.Message)
}

// ErrConsumerClosed is returned from consumer.Err() when the consumer was
// explicitly closed by the caller via consumer.Close().
// Distinct from connection errors so callers can distinguish "I closed it"
// from "it died unexpectedly".
type ErrConsumerClosed struct{}

func (e *ErrConsumerClosed) Error() string {
	return "streamq: consumer closed"
}

// ErrMaxReconnects is returned when a consumer exhausts its reconnect
// attempts after repeated connection failures.
type ErrMaxReconnects struct {
	Attempts int
	Last     error
}

func (e *ErrMaxReconnects) Error() string {
	return fmt.Sprintf("streamq: consumer gave up after %d reconnect attempts: %v", e.Attempts, e.Last)
}

func (e *ErrMaxReconnects) Unwrap() error {
	return e.Last
}

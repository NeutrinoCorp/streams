package streamhub

import "context"

// Listener is a wrapping structure of the ListenFunc handler for complex data processing scenarios.
type Listener interface {
	// Listen starts the execution process triggered when a message is received from a stream.
	//
	// Returns an error to indicate the process has failed so Hub will retry the processing using exponential backoff.
	Listen(ctx context.Context, message []byte) error
}

// ListenerFunc is the execution process triggered when a message is received from a stream.
//
// Returns an error to indicate the process has failed so Hub will retry the processing using exponential backoff.
type ListenerFunc func(ctx context.Context, message []byte) error

package streamhub

import "context"

// Listener is a wrapping structure of the ListenFunc handler for complex data processing scenarios.
type Listener interface {
	// Listen starts the execution process triggered when a message is received from a stream.
	//
	// Returns an error to indicate the process has failed so Hub will retry the processing using exponential backoff.
	Listen(ctx context.Context, message Message) error
}

// ListenerFunc is the execution process triggered when a message is received from a stream.
//
// Returns an error to indicate the process has failed so Hub will retry the processing using exponential backoff.
type ListenerFunc func(ctx context.Context, message Message) error

// ListenerNoop the no-operation implementation of Listener
type ListenerNoop struct{}

var _ Listener = ListenerNoop{}

// Listen the no-operation implementation of Listener.Listen()
func (l ListenerNoop) Listen(_ context.Context, _ Message) error {
	return nil
}

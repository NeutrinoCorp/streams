package streamhub

import "context"

// PublisherFunc inserts a message into a stream assigned to the message in the StreamRegistry in order to propagate the
// data to a set of subscribed systems for further processing.
type PublisherFunc func(ctx context.Context, message Message) error

// Publisher is a wrapping structure of PublishMessageFunc for complex message publishing scenarios.
type Publisher interface {
	// Publish inserts a message into a stream assigned to the message in the StreamRegistry in order to propagate the
	// data to a set of subscribed systems for further processing.
	Publish(ctx context.Context, message Message) error
}

// NoopPublisher is the no-operation implementation of PublishMessageFunc
var NoopPublisher PublisherFunc = func(ctx context.Context, message Message) error {
	return nil
}

package streamhub

import (
	"context"
)

// Publisher inserts messages into streams assigned on the StreamRegistry in order to propagate the
// data to a set of subscribed systems for further processing.
//
// This type should be provided by a streamhub Driver (e.g. Apache Pulsar, Apache Kafka, Amazon SNS)
type Publisher interface {
	// Publish inserts a message into a stream assigned to the message in the StreamRegistry in order to propagate the
	// data to a set of subscribed systems for further processing.
	Publish(ctx context.Context, message Message) error
	// PublishBatch inserts a set of messages into a stream assigned to the message in the StreamRegistry in order to propagate the
	// data to a set of subscribed systems for further processing.
	//
	// Depending on the underlying Publisher driver implementation, this function MIGHT return an error if a single operation failed,
	// or it MIGHT return an error if the whole operation failed
	PublishBatch(ctx context.Context, messages ...Message) error
}

type noopPublisher struct{}

// NoopPublisher is the no-operation implementation of Publisher
var NoopPublisher Publisher = noopPublisher{}

// Publish is the no-operation implementation of Publisher.Publish()
func (n noopPublisher) Publish(_ context.Context, _ Message) error {
	return nil
}

// PublishBatch is the no-op implementation of Publisher.PublishBatch()
func (n noopPublisher) PublishBatch(_ context.Context, _ ...Message) error {
	return nil
}

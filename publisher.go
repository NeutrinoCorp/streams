package streamhub

import (
	"context"
)

// PublisherFunc inserts a message into a stream assigned to the message in the StreamRegistry in order to propagate the
// data to a set of subscribed systems for further processing.
//
// This type should be provided by a streamhub Driver (e.g. Apache Pulsar, Apache Kafka, Amazon SNS)
type PublisherFunc func(ctx context.Context, message Message) error

// Publisher is a wrapping structure of PublishMessageFunc for complex message publishing scenarios.
//
// This type should be provided by a streamhub Driver (e.g. Apache Pulsar, Apache Kafka, Amazon SNS)
type Publisher interface {
	// Publish inserts a message into a stream assigned to the message in the StreamRegistry in order to propagate the
	// data to a set of subscribed systems for further processing.
	Publish(ctx context.Context, message Message) error
}

// NoopPublisherFunc is the no-operation implementation of PublisherFunc
var NoopPublisherFunc PublisherFunc = func(ctx context.Context, message Message) error {
	return nil
}

type noopPublisher struct{}

// NoopPublisher is the no-operation implementation of Publisher
var NoopPublisher Publisher = noopPublisher{}

// Publish is the no-operation implementation of Publisher.Publish()
func (n noopPublisher) Publish(_ context.Context, _ Message) error {
	return nil
}

package streamhub

import (
	"context"
)

// Writer inserts messages into streams assigned on the StreamRegistry in order to propagate the
// data to a set of subscribed systems for further processing.
//
// This type should be provided by a streamhub Driver (e.g. Apache Pulsar, Apache Kafka, Amazon SNS)
type Writer interface {
	// Write inserts a message into a stream assigned to the message in the StreamRegistry in order to propagate the
	// data to a set of subscribed systems for further processing.
	Write(ctx context.Context, message Message) error
	// WriteBatch inserts a set of messages into a stream assigned to the message in the StreamRegistry in order to propagate the
	// data to a set of subscribed systems for further processing.
	//
	// Depending on the underlying Writer driver implementation, this function MIGHT return an error if a single operation failed,
	// or it MIGHT return an error if the whole operation failed
	WriteBatch(ctx context.Context, messages ...Message) error
}

type noopWriter struct{}

// NoopWriter is the no-operation implementation of Writer
var NoopWriter Writer = noopWriter{}

// Write is the no-operation implementation of Writer.Write()
func (n noopWriter) Write(_ context.Context, _ Message) error {
	return nil
}

// WriteBatch is the no-op implementation of Writer.WriteBatch()
func (n noopWriter) WriteBatch(_ context.Context, _ ...Message) error {
	return nil
}

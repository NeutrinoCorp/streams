package streams

import (
	"context"
	"errors"
)

var (
	// DefaultHub is the `streams` base instance used by the `streams` simple API. Recommended to use where only one
	// hub instance is required.
	//
	// This variable MUST be allocated manually.
	DefaultHub *Hub
	// ErrNilDefaultHub DefaultHub has not been initialized.
	ErrNilDefaultHub = errors.New("streams: DefaultHub has not been initialized")
)

func checkDefaultHubInstance() {
	if DefaultHub == nil {
		panic(ErrNilDefaultHub)
	}
}

// RegisterStream creates a relation between a stream message type and metadata.
//
// If registering a Google's Protocol Buffer message, DO NOT use a pointer as message schema
// to avoid marshaling problems
func RegisterStream(message interface{}, metadata StreamMetadata) {
	checkDefaultHubInstance()
	DefaultHub.RegisterStream(message, metadata)
}

// RegisterStreamByString creates a relation between a string key and metadata.
func RegisterStreamByString(messageType string, metadata StreamMetadata) {
	checkDefaultHubInstance()
	DefaultHub.RegisterStreamByString(messageType, metadata)
}

// Read registers a new stream-reading background job.
//
// If reading from a Google's Protocol Buffer message pipeline, DO NOT use a pointer as message schema
// to avoid marshaling problems
func Read(message interface{}, opts ...ReaderNodeOption) error {
	checkDefaultHubInstance()
	return DefaultHub.Read(message, opts...)
}

// ReadByStreamKey registers a new stream-reading background job using the raw stream identifier (e.g. topic name).
func ReadByStreamKey(stream string, opts ...ReaderNodeOption) {
	checkDefaultHubInstance()
	DefaultHub.ReadByStreamKey(stream, opts...)
}

// Start initiates all daemons (e.g. stream-reading jobs) processes
func Start(ctx context.Context) {
	checkDefaultHubInstance()
	DefaultHub.Start(ctx)
}

// Write inserts a message into a stream assigned to the message in the StreamRegistry in order to propagate the
// data to a set of subscribed systems for further processing.
//
// Uses given context to inject correlation and causation IDs.
func Write(ctx context.Context, message interface{}) error {
	checkDefaultHubInstance()
	return DefaultHub.Write(ctx, message)
}

// WriteBatch inserts a set of messages into a stream assigned on the StreamRegistry in order to propagate the
// data to a set of subscribed systems for further processing.
//
// Uses given context to inject correlation and causation IDs.
//
// If an item from the batch fails, other items will fail too
func WriteBatch(ctx context.Context, messages ...interface{}) (uint32, error) {
	checkDefaultHubInstance()
	return DefaultHub.WriteBatch(ctx, messages...)
}

// WriteByMessageKey inserts a message into a stream using the custom message key from StreamRegistry in order to
// propagate the data to a set of subscribed systems for further processing.
//
// Uses given context to inject correlation and causation IDs.
func WriteByMessageKey(ctx context.Context, messageKey string, message interface{}) error {
	checkDefaultHubInstance()
	return DefaultHub.WriteByMessageKey(ctx, messageKey, message)
}

// WriteByMessageKeyBatch inserts a set of messages into a stream using the custom message key from StreamRegistry in order to
// propagate the data to a set of subscribed systems for further processing.
//
// Uses given context to inject correlation and causation IDs.
//
// If an item from the batch fails, other items will fail too
func WriteByMessageKeyBatch(ctx context.Context, items WriteByMessageKeyBatchItems) (uint32, error) {
	checkDefaultHubInstance()
	return DefaultHub.WriteByMessageKeyBatch(ctx, items)
}

// WriteRawMessage inserts a raw transport message into a stream in order to propagate the data to a set
// of subscribed systems for further processing.
//
// Uses given context to inject correlation and causation IDs.
func WriteRawMessage(ctx context.Context, message Message) error {
	checkDefaultHubInstance()
	return DefaultHub.WriteRawMessage(ctx, message)
}

// WriteRawMessageBatch inserts a set of raw transport message into a stream in order to propagate the data to a set
// of subscribed systems for further processing.
//
// Uses given context to inject correlation and causation IDs.
//
// The whole batch will be passed to the underlying Writer driver implementation as every driver has its own way to
// deal with batches
func WriteRawMessageBatch(ctx context.Context, messages ...Message) (uint32, error) {
	checkDefaultHubInstance()
	return DefaultHub.WriteRawMessageBatch(ctx, messages...)
}

// GetStreamReaderNodes retrieves ReaderNode(s) from a stream.
func GetStreamReaderNodes(stream string) []ReaderNode {
	checkDefaultHubInstance()
	list := DefaultHub.GetStreamReaderNodes(stream)
	if list == nil {
		return nil
	}
	res := make([]ReaderNode, 0, list.Size())
	for _, item := range list.Values() {
		node := item.(ReaderNode)
		res = append(res, node)
	}
	return res
}

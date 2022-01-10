package streamhub

import (
	"errors"
	"reflect"
)

// ErrMissingStream the requested stream was not found in the StreamRegistry
var ErrMissingStream = errors.New("streamhub: Missing stream entry in stream registry")

// StreamMetadata contains information of stream messages.
type StreamMetadata struct {
	Stream           string
	SchemaDefinition string
	SchemaVersion    int
}

// StreamRegistry is an in-memory storage of message metadata used by Hub and any external agent to set and
// retrieve information about a stream message.
//
// Uses the message type (or a custom string) as key.
type StreamRegistry map[string]StreamMetadata

// Set creates a relation between a stream message type and metadata.
func (r StreamRegistry) Set(message interface{}, metadata StreamMetadata) {
	msgType := reflect.TypeOf(message).String()
	r.SetByString(msgType, metadata)
}

// SetByString creates a relation between a string key and metadata.
func (r StreamRegistry) SetByString(key string, metadata StreamMetadata) {
	r[key] = metadata
}

// Get retrieves a stream message metadata from a stream message type.
func (r StreamRegistry) Get(message interface{}) (StreamMetadata, error) {
	msgType := reflect.TypeOf(message).String()
	return r.GetByString(msgType)
}

// GetByString retrieves a stream message metadata from a string key.
func (r StreamRegistry) GetByString(key string) (StreamMetadata, error) {
	metadata, ok := r[key]
	if !ok {
		return StreamMetadata{}, ErrMissingStream
	}
	return metadata, nil
}

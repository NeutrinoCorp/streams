package streamhub

import (
	"errors"
	"reflect"
)

// ErrMissingStream the requested stream was not found in the StreamRegistry
var ErrMissingStream = errors.New("streamhub: Missing stream entry in stream registry")

// StreamMetadata contains information of stream messages.
type StreamMetadata struct {
	Stream               string
	SchemaDefinitionName string
	SchemaVersion        int
	GoType               reflect.Type
}

// StreamRegistry is an in-memory storage of streams metadata used by Hub and any external agent to set and
// retrieve information about a specific stream.
//
// Uses a custom string (or Go's struct type as string) as key.
type StreamRegistry map[string]StreamMetadata

// Set creates a relation between a stream message type and metadata.
func (r StreamRegistry) Set(message interface{}, metadata StreamMetadata) {
	msgType := reflect.TypeOf(message)
	metadata.GoType = msgType
	r.SetByString(msgType.String(), metadata)
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

// GetByStreamName retrieves a stream message metadata from a stream name.
//
// It contains an optimistic lookup mechanism to keep constant time and space complexity.
//
// If metadata is not found by the given key, then fallback default to O(n) lookup.
// This will increase time and space complexity of the fallback function by the GetByString base complexity.
// Nevertheless, GetByString will be always constant, so it is guaranteed to keep
// a constant complexity sum to the overall GetByStream complexity.
// E.g. GetByString = 49.75 ns/op, hence, GetByStreamName = original ns/op + GetByString ns/op.
//
// This optimistic lookup is done in order to keep amortized time and space complexity when using non-reflection
// based implementations on the root Hub (using only String methods from this very Stream Registry component). Thus,
// greater performance is achieved for scenarios when reflection-based stream registration is not required by the program.
func (r StreamRegistry) GetByStreamName(name string) (StreamMetadata, error) {
	if metadata, err := r.GetByString(name); err == nil {
		return metadata, nil
	}

	for _, stream := range r {
		if stream.Stream == name {
			return stream, nil
		}
	}

	return StreamMetadata{}, ErrMissingStream
}

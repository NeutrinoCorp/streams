package streams

import (
	"errors"
	"strings"

	"github.com/modern-go/reflect2"
)

// ErrMissingStream the requested stream was not found in the StreamRegistry
var ErrMissingStream = errors.New("streams: Missing stream entry in stream registry")

// StreamMetadata contains information of stream messages.
type StreamMetadata struct {
	// Stream destination stream name (aka. topic)
	Stream string
	// StreamVersion destination stream major version. Useful when non-backwards compatible schema update is desired.
	StreamVersion int
	// SchemaDefinitionName
	SchemaDefinitionName string
	SchemaVersion        int
	GoType               reflect2.Type
}

// StreamRegistry is an in-memory storage of streams metadata used by Hub and any external agent to set and
// retrieve information about a specific stream.
//
// Uses a custom string (or Go's struct type as string) as key.
//
// Note: A message key differs from stream name as the message key COULD be anything the developer sets within the
// stream registry. Thus, scenarios where multiple data types require publishing messages to the same stream are possible.
// Moreover, the message key is set by reflection-based registries with the reflect.TypeOf function, so it will differ
// from the actual stream name.
type StreamRegistry map[string]StreamMetadata

// Set creates a relation between a stream message type and metadata.
func (r StreamRegistry) Set(message interface{}, metadata StreamMetadata) {
	msgType := reflect2.TypeOf(message)
	metadata.GoType = msgType
	r.SetByString(strings.Trim(msgType.String(), "*"), metadata)
}

// SetByString creates a relation between a string key and metadata.
func (r StreamRegistry) SetByString(key string, metadata StreamMetadata) {
	r[key] = metadata
}

// Get retrieves a stream message metadata from a stream message type.
func (r StreamRegistry) Get(message interface{}) (StreamMetadata, error) {
	msgType := strings.Trim(reflect2.TypeOf(message).String(), "*")
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
// E.g. GetByString = 49.75 ns/op, therefore GetByStreamName = original ns/op + GetByString ns/op.
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

package streamhub

import jsoniter "github.com/json-iterator/go"

// Marshaler handles data transformation between primitives and specific codecs/formats (e.g. JSON, Apache Avro).
type Marshaler interface {
	// Marshal transforms a complex data type into a primitive binary array for data transportation.
	Marshal(data interface{}) ([]byte, error)
	// Unmarshal transforms a primitive binary array to a complex data type for data processing.
	Unmarshal(data []byte, ref interface{}) error
	// ContentType retrieves the encoding/decoding format using RFC 2046 standard (e.g. application/json).
	ContentType() string
}

type JSONMarshaler struct{}

var _ Marshaler = JSONMarshaler{}

// Marshal transforms a complex data type into a primitive binary array for data transportation using JSON codec.
func (m JSONMarshaler) Marshal(data interface{}) ([]byte, error) {
	return jsoniter.Marshal(data)
}

// Unmarshal transforms a primitive binary array to a complex data type for data processing using JSON codec.
func (m JSONMarshaler) Unmarshal(data []byte, ref interface{}) error {
	return jsoniter.Unmarshal(data, ref)
}

// ContentType retrieves the encoding/decoding JSON format using RFC 2046 standard (e.g. application/json).
func (m JSONMarshaler) ContentType() string {
	return "application/json"
}

package streamhub

import (
	"github.com/hamba/avro"
	jsoniter "github.com/json-iterator/go"
)

// Marshaler handles data transformation between primitives and specific codecs/formats (e.g. JSON, Apache Avro).
type Marshaler interface {
	// Marshal transforms a complex data type into a primitive binary array for data transportation.
	Marshal(schemaDef string, data interface{}) ([]byte, error)
	// Unmarshal transforms a primitive binary array to a complex data type for data processing.
	Unmarshal(schemaDef string, data []byte, ref interface{}) error
	// ContentType retrieves the encoding/decoding format using RFC 2046 standard (e.g. application/json).
	ContentType() string
}

// JSONMarshaler handles data transformation between primitives and JSON format.
type JSONMarshaler struct{}

var _ Marshaler = JSONMarshaler{}

// Marshal transforms a complex data type into a primitive binary array for data transportation using JSON format.
func (m JSONMarshaler) Marshal(_ string, data interface{}) ([]byte, error) {
	return jsoniter.Marshal(data)
}

// Unmarshal transforms a primitive binary array to a complex data type for data processing using JSON format.
func (m JSONMarshaler) Unmarshal(_ string, data []byte, ref interface{}) error {
	return jsoniter.Unmarshal(data, ref)
}

// ContentType retrieves the encoding/decoding JSON format using RFC 2046 standard (application/json).
func (m JSONMarshaler) ContentType() string {
	return "application/json"
}

// AvroMarshaler handles data transformation between primitives and Apache Avro format.
//
// Apache Avro REQUIRES a defined SchemaRegistry to decode/encode data.
type AvroMarshaler struct{}

var _ Marshaler = AvroMarshaler{}

// Marshal transforms a complex data type into a primitive binary array for data transportation using Apache Avro format.
func (a AvroMarshaler) Marshal(schemaDef string, data interface{}) ([]byte, error) {
	schemaAvro, err := avro.Parse(schemaDef)
	if err != nil {
		return nil, err
	}
	return avro.Marshal(schemaAvro, data)

}

// Unmarshal transforms a primitive binary array to a complex data type for data processing using Apache Avro format.
func (a AvroMarshaler) Unmarshal(schemaDef string, data []byte, ref interface{}) error {
	schemaAvro, err := avro.Parse(schemaDef)
	if err != nil {
		return err
	}
	return avro.Unmarshal(schemaAvro, data, ref)
}

// ContentType retrieves the encoding/decoding Apache Avro format using RFC 2046 standard (application/avro).
func (a AvroMarshaler) ContentType() string {
	return "application/avro"
}

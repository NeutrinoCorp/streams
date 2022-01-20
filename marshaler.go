package streamhub

import (
	"hash/fnv"

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
type AvroMarshaler struct {
	cache map[uint64]avro.Schema
}

// NewAvroMarshaler allocates a new Apache Avro marshaler with a simple caching system to reduce memory footprint and
// computational usage when parsing Avro schema definition files.
func NewAvroMarshaler() AvroMarshaler {
	return AvroMarshaler{
		cache: make(map[uint64]avro.Schema, 0),
	}
}

var _ Marshaler = AvroMarshaler{}

// Marshal transforms a complex data type into a primitive binary array for data transportation using Apache Avro format.
func (a AvroMarshaler) Marshal(schemaDef string, data interface{}) (parsedData []byte, err error) {
	var schemaAvro avro.Schema
	if a.cache != nil {
		var ok bool
		hashingAlgorithm := fnv.New64a()
		_, err = hashingAlgorithm.Write([]byte(schemaDef))
		if err != nil {
			return nil, err
		}
		hashKey := hashingAlgorithm.Sum64()
		schemaAvro, ok = a.cache[hashKey]
		defer func(specFound bool) {
			if !ok {
				a.cache[hashKey] = schemaAvro
			}
		}(ok)
	}

	if schemaAvro == nil {
		schemaAvro, err = avro.Parse(schemaDef)
		if err != nil {
			return nil, err
		}
	}
	parsedData, err = avro.Marshal(schemaAvro, data)
	return
}

// Unmarshal transforms a primitive binary array to a complex data type for data processing using Apache Avro format.
func (a AvroMarshaler) Unmarshal(schemaDef string, data []byte, ref interface{}) (err error) {
	var schemaAvro avro.Schema
	if a.cache != nil {
		var ok bool
		hashingAlgorithm := fnv.New64a()
		_, err = hashingAlgorithm.Write([]byte(schemaDef))
		if err != nil {
			return err
		}
		hashKey := hashingAlgorithm.Sum64()
		schemaAvro, ok = a.cache[hashKey]
		defer func(specFound bool) {
			if !ok {
				a.cache[hashKey] = schemaAvro
			}
		}(ok)
	}

	if schemaAvro == nil {
		schemaAvro, err = avro.Parse(schemaDef)
		if err != nil {
			return err
		}
	}
	err = avro.Unmarshal(schemaAvro, data, ref)
	return
}

// ContentType retrieves the encoding/decoding Apache Avro format using RFC 2046 standard (application/avro).
func (a AvroMarshaler) ContentType() string {
	return "application/avro"
}

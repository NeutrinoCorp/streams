package streamhub

import (
	"errors"
	"hash"
	"hash/fnv"
	"sync"

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

// FailingMarshalerNoop the no-operation failing Marshaler
//
// For testing purposes only
type FailingMarshalerNoop struct{}

var _ Marshaler = FailingMarshalerNoop{}

// Marshal the failing marshal operation
func (f FailingMarshalerNoop) Marshal(_ string, _ interface{}) ([]byte, error) {
	return nil, errors.New("failing marshal")
}

// Unmarshal the failing unmarshal operation
func (f FailingMarshalerNoop) Unmarshal(_ string, _ []byte, _ interface{}) error {
	return errors.New("failing unmarshal")
}

// ContentType the failing content type operation
func (f FailingMarshalerNoop) ContentType() string {
	return ""
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
	cache          *sync.Map
	HashingFactory Hashing64AlgorithmFactory
}

// NewAvroMarshaler allocates a new Apache Avro marshaler with a simple caching system to reduce memory footprint and
// computational usage when parsing Avro schema definition files.
func NewAvroMarshaler() AvroMarshaler {
	return AvroMarshaler{
		cache:          new(sync.Map),
		HashingFactory: DefaultHashing64AlgorithmFactory,
	}
}

var _ Marshaler = AvroMarshaler{}

// Hashing64AlgorithmFactory factory for hash.Hash64 algorithms (used by Apache Avro schema definition caching system)
type Hashing64AlgorithmFactory func() hash.Hash64

// DefaultHashing64AlgorithmFactory the default hashing64 algorithm factory for Marshaler schema definition caching layer
var DefaultHashing64AlgorithmFactory Hashing64AlgorithmFactory = func() hash.Hash64 {
	return fnv.New64a()
}

// Marshal transforms a complex data type into a primitive binary array for data transportation using Apache Avro format.
func (a AvroMarshaler) Marshal(schemaDef string, data interface{}) (parsedData []byte, err error) {
	var schemaAvro avro.Schema
	if a.cache != nil {
		var ok bool
		hashingAlgorithm := a.HashingFactory()
		_, err = hashingAlgorithm.Write([]byte(schemaDef))
		if err != nil {
			return nil, err
		}
		hashKey := hashingAlgorithm.Sum64()
		var schemaAvroMap interface{}
		schemaAvroMap, ok = a.cache.Load(hashKey)
		if ok {
			schemaAvro = schemaAvroMap.(avro.Schema)
		}
		defer func(specFound bool) {
			if !ok {
				a.cache.Store(hashKey, schemaAvro)
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
		hashingAlgorithm := a.HashingFactory()
		_, err = hashingAlgorithm.Write([]byte(schemaDef))
		if err != nil {
			return err
		}
		hashKey := hashingAlgorithm.Sum64()
		var schemaAvroMap interface{}
		schemaAvroMap, ok = a.cache.Load(hashKey)
		if ok {
			schemaAvro = schemaAvroMap.(avro.Schema)
		}
		defer func(specFound bool) {
			if !ok {
				a.cache.Store(hashKey, schemaAvro)
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

package streamhub

import (
	"errors"
	"strconv"
	"strings"
)

// ErrMissingSchemaDefinition the requested stream message definition was not found in the SchemaRegistry
var ErrMissingSchemaDefinition = errors.New("streamhub: Missing stream schema definition in schema registry")

// SchemaRegistry is an external storage of stream message schemas definitions with proper versioning.
//
// Examples of this schema registries are Amazon Glue Schema Registry and Confluent Schema Registry.
type SchemaRegistry interface {
	// GetSchemaDefinition retrieves a schema definition (in string format) from the registry
	GetSchemaDefinition(name string, version int) (string, error)
}

// NoopSchemaRegistry is the no-operation implementation of SchemaRegistry
type NoopSchemaRegistry struct{}

var _ SchemaRegistry = NoopSchemaRegistry{}

// GetSchemaDefinition retrieves an empty string and a nil error
func (n NoopSchemaRegistry) GetSchemaDefinition(_ string, _ int) (string, error) {
	return "", nil
}

// InMemorySchemaRegistry is the in memory schema registry, crafted specially for basic and/or testing scenarios.
type InMemorySchemaRegistry map[string]string

var _ SchemaRegistry = InMemorySchemaRegistry{}

// RegisterDefinition stores the given schema definition into the registry
func (i InMemorySchemaRegistry) RegisterDefinition(name, def string, version int) {
	key := name
	if version > 0 {
		var buff strings.Builder
		buff.WriteString(key)
		buff.WriteString("#")
		buff.WriteString(strconv.Itoa(version))
		key = buff.String()
	}
	i[key] = def
}

// GetSchemaDefinition retrieves a schema definition (in string format) from the registry
func (i InMemorySchemaRegistry) GetSchemaDefinition(name string, version int) (string, error) {
	var buffKey strings.Builder
	buffKey.WriteString(name)
	if version > 0 {
		buffKey.WriteString("#")
		buffKey.WriteString(strconv.Itoa(version))
	}
	def, ok := i[buffKey.String()]
	if !ok {
		return "", ErrMissingSchemaDefinition
	}
	return def, nil
}

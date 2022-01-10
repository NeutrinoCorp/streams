package streamhub

import (
	"errors"
	"strconv"
)

// ErrMissingSchemaDefinition the requested stream message definition was not found in the SchemaRegistry
var ErrMissingSchemaDefinition = errors.New("streamhub: Missing stream schema definition in schema registry")

// SchemaRegistry is an external storage of stream message schemas definitions with proper versioning.
//
// Examples of this schema registries are Amazon Glue Schema Registry and Confluent Schema Registry.
type SchemaRegistry interface {
	// GetSchemaDefinition retrieves the schema definition in string format.
	GetSchemaDefinition(name string, version int) (string, error)
}

// NoopSchemaRegistry is the no-operation implementation of SchemaRegistry
type NoopSchemaRegistry struct{}

var _ SchemaRegistry = NoopSchemaRegistry{}

func (n NoopSchemaRegistry) GetSchemaDefinition(_ string, _ int) (string, error) {
	return "", nil
}

// InMemorySchemaRegistry is the in memory schema registry, crafted specially for basic and/or testing scenarios.
type InMemorySchemaRegistry map[string]string

var _ SchemaRegistry = InMemorySchemaRegistry{}

func (i InMemorySchemaRegistry) RegisterDefinition(name, def string, version int) {
	key := name
	if version > 0 {
		key += "#" + strconv.Itoa(version)
	}
	i[key] = def
}

func (i InMemorySchemaRegistry) GetSchemaDefinition(name string, version int) (string, error) {
	key := name
	if version > 0 {
		key += "#" + strconv.Itoa(version)
	}
	def, ok := i[key]
	if !ok {
		return "", ErrMissingSchemaDefinition
	}
	return def, nil
}

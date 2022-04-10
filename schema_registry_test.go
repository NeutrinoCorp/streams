package streams_test

import (
	"testing"

	"github.com/neutrinocorp/streams"
	"github.com/stretchr/testify/assert"
)

func TestInMemorySchemaRegistry(t *testing.T) {
	r := streams.InMemorySchemaRegistry{}
	r.RegisterDefinition("foo", `{
		"type": "record",
		"name": "foo",
		"namespace": "org.ncorp.avro",
		"fields" : [
			{"name": "a", "type": "long"},
			{"name": "b", "type": "string"}
		]
	}`, 0)
	r.RegisterDefinition("bar", `{
		"type": "record",
		"name": "bar",
		"namespace": "org.ncorp.avro",
		"fields" : [
			{"name": "a", "type": "long"},
			{"name": "b", "type": "string"}
		]
	}`, 1)
	def, err := r.GetSchemaDefinition("foo", 0)
	assert.NoError(t, err)
	assert.NotEmpty(t, def)

	def, err = r.GetSchemaDefinition("bar", 1)
	assert.NoError(t, err)
	assert.NotEmpty(t, def)

	def, err = r.GetSchemaDefinition("bar", 0)
	assert.ErrorIs(t, err, streams.ErrMissingSchemaDefinition)
	assert.Empty(t, def)

	def, err = r.GetSchemaDefinition("baz", 1)
	assert.ErrorIs(t, err, streams.ErrMissingSchemaDefinition)
	assert.Empty(t, def)
}

func BenchmarkInMemorySchemaRegistry_GetSchemaDefinition(b *testing.B) {
	r := streams.InMemorySchemaRegistry{}
	r.RegisterDefinition("foo", `{
		"type": "record",
		"name": "foo",
		"namespace": "org.ncorp.avro",
		"fields" : [
			{"name": "a", "type": "long"},
			{"name": "b", "type": "string"}
		]
	}`, 0)
	r.RegisterDefinition("bar", `{
		"type": "record",
		"name": "bar",
		"namespace": "org.ncorp.avro",
		"fields" : [
			{"name": "a", "type": "long"},
			{"name": "b", "type": "string"}
		]
	}`, 1)
	for i := 0; i < b.N; i++ {
		b.ReportAllocs()
		_, _ = r.GetSchemaDefinition("foo", 0)
	}
}

func BenchmarkInMemorySchemaRegistry_RegisterDefinition(b *testing.B) {
	r := streams.InMemorySchemaRegistry{}
	for i := 0; i < b.N; i++ {
		b.ReportAllocs()
		r.RegisterDefinition("foo", `{
		"type": "record",
		"name": "foo",
		"namespace": "org.ncorp.avro",
		"fields" : [
			{"name": "a", "type": "long"},
			{"name": "b", "type": "string"}
			]
		}`, i)
	}
}

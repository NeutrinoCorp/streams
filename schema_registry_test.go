package streamhub_test

import (
	"testing"

	"github.com/neutrinocorp/streamhub"
	"github.com/stretchr/testify/assert"
)

func TestInMemorySchemaRegistry(t *testing.T) {
	r := streamhub.InMemorySchemaRegistry{}
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
	assert.ErrorIs(t, err, streamhub.ErrMissingSchemaDefinition)
	assert.Empty(t, def)

	def, err = r.GetSchemaDefinition("baz", 1)
	assert.ErrorIs(t, err, streamhub.ErrMissingSchemaDefinition)
	assert.Empty(t, def)
}

package streamhub_test

import (
	"testing"

	"github.com/neutrinocorp/streamhub"
	"github.com/stretchr/testify/assert"
)

func TestJSONMarshaler_Marshal(t *testing.T) {
	msg := fooMessage{Foo: "foo"}
	m := streamhub.JSONMarshaler{}
	data, err := m.Marshal("", msg)
	assert.NotNil(t, data)
	assert.NoError(t, err)
}

func TestJSONMarshaler_Unmarshal(t *testing.T) {
	msg := fooMessage{Foo: "foo"}
	m := streamhub.JSONMarshaler{}
	data, err := m.Marshal("", msg)
	assert.NoError(t, err)
	assert.NotNil(t, data)
	msgRef := fooMessage{}
	err = m.Unmarshal("", data, &msgRef)
	assert.NoError(t, err)
	assert.NotEmpty(t, msgRef)
}

func TestJSONMarshaler_ContentType(t *testing.T) {
	m := streamhub.JSONMarshaler{}
	assert.Equal(t, "application/json", m.ContentType())
}

func TestAvroMarshaler_Marshal(t *testing.T) {
	msg := fooMessage{Foo: "foo"}
	m := streamhub.AvroMarshaler{}
	data, err := m.Marshal("", msg)
	assert.Nil(t, data)
	assert.Error(t, err)

	data, err = m.Marshal(`{
		"type": "record",
		"name": "fooMessage",
		"namespace": "org.ncorp.avro",
		"fields" : [
			{"name": "foo", "type": "string"}
		]
	}`, msg)
	assert.NotNil(t, data)
	assert.NoError(t, err)
}

func TestAvroMarshaler_Unmarshal(t *testing.T) {
	def := `{
		"type": "record",
		"name": "fooMessage",
		"namespace": "org.ncorp.avro",
		"fields" : [
			{"name": "foo", "type": "string"}
		]
	}`
	msg := fooMessage{Foo: "foo"}
	m := streamhub.AvroMarshaler{}
	data, err := m.Marshal(def, msg)
	assert.NotNil(t, data)
	assert.NoError(t, err)
	msgRef := fooMessage{}

	err = m.Unmarshal("", data, &msgRef)
	assert.Error(t, err)
	assert.Empty(t, msgRef)

	err = m.Unmarshal(def, data, &msgRef)
	assert.NoError(t, err)
	assert.NotEmpty(t, msgRef)
}

func TestAvroMarshaler_ContentType(t *testing.T) {
	m := streamhub.AvroMarshaler{}
	assert.Equal(t, "application/avro", m.ContentType())
}

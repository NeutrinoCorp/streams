package streamhub_test

import (
	"testing"

	"github.com/neutrinocorp/streamhub"
	"github.com/stretchr/testify/assert"
)

func TestJSONMarshaler_Marshal(t *testing.T) {
	msg := fooMessage{Foo: "foo"}
	m := streamhub.JSONMarshaler{}
	data, err := m.Marshal(msg)
	assert.NotNil(t, data)
	assert.NoError(t, err)
}

func TestJSONMarshaler_Unmarshal(t *testing.T) {
	msg := fooMessage{Foo: "foo"}
	m := streamhub.JSONMarshaler{}
	data, err := m.Marshal(msg)
	assert.NoError(t, err)
	assert.NotNil(t, data)
	msgRef := fooMessage{}
	err = m.Unmarshal(data, &msgRef)
	assert.NoError(t, err)
	assert.NotEmpty(t, msgRef)
}

func TestJSONMarshaler_ContentType(t *testing.T) {
	m := streamhub.JSONMarshaler{}
	assert.Equal(t, "application/json", m.ContentType())
}

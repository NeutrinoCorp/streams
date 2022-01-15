package streamhub_test

import (
	"errors"
	"testing"

	"github.com/neutrinocorp/streamhub"
	"github.com/stretchr/testify/assert"
)

type failingFakeMarshaler struct{}

var _ streamhub.Marshaler = failingFakeMarshaler{}

func (f failingFakeMarshaler) Marshal(_ string, _ interface{}) ([]byte, error) {
	return nil, errors.New("generic marshal error")
}

func (f failingFakeMarshaler) Unmarshal(_ string, _ []byte, _ interface{}) error {
	return errors.New("generic unmarshal error")
}

func (f failingFakeMarshaler) ContentType() string {
	return ""
}

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

func BenchmarkJSONMarshaler_Marshal(b *testing.B) {
	msg := fooMessage{Foo: "foo"}
	m := streamhub.JSONMarshaler{}
	for i := 0; i < b.N; i++ {
		b.ReportAllocs()
		_, _ = m.Marshal("", msg)
	}
}

func BenchmarkJSONMarshaler_Unmarshal(b *testing.B) {
	msg := fooMessage{Foo: "foo"}
	m := streamhub.JSONMarshaler{}
	data, _ := m.Marshal("", msg)
	ref := &fooMessage{}
	for i := 0; i < b.N; i++ {
		b.ReportAllocs()
		_ = m.Unmarshal("", data, ref)
	}
}

func TestAvroMarshaler_Marshal(t *testing.T) {
	msg := fooMessage{Foo: "foo"}
	m := streamhub.NewAvroMarshaler()
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
	m := streamhub.NewAvroMarshaler()
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

func BenchmarkAvroMarshaler_Marshal(b *testing.B) {
	msg := fooMessage{Foo: "foo"}
	m := streamhub.NewAvroMarshaler()

	for i := 0; i < b.N; i++ {
		b.ReportAllocs()
		_, _ = m.Marshal(`{
			"type": "record",
			"name": "fooMessage",
			"namespace": "org.ncorp.avro",
			"fields" : [
				{"name": "foo", "type": "string"}
			]
		}`, msg)
	}
}

func BenchmarkAvroMarshaler_Unmarshal(b *testing.B) {
	def := `{
			"type": "record",
			"name": "fooMessage",
			"namespace": "org.ncorp.avro",
			"fields" : [
				{"name": "foo", "type": "string"}
			]
		}`
	msg := fooMessage{Foo: "foo"}
	m := streamhub.NewAvroMarshaler()
	data, _ := m.Marshal(def, msg)

	ref := &fooMessage{}
	for i := 0; i < b.N; i++ {
		b.ReportAllocs()
		_ = m.Unmarshal(def, data, ref)
	}
}

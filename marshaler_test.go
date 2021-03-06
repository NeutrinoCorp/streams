package streams_test

import (
	"errors"
	"hash"
	"testing"
	"time"

	"github.com/neutrinocorp/streams"
	"github.com/neutrinocorp/streams/testdata/proto/examplepb"
	"github.com/stretchr/testify/assert"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type failingFakeMarshaler struct{}

var _ streams.Marshaler = failingFakeMarshaler{}

func (f failingFakeMarshaler) Marshal(_ string, _ interface{}) ([]byte, error) {
	return nil, errors.New("generic marshal error")
}

func (f failingFakeMarshaler) Unmarshal(_ string, _ []byte, _ interface{}) error {
	return errors.New("generic unmarshal error")
}

func (f failingFakeMarshaler) ContentType() string {
	return ""
}

func TestFailingMarshaler_ContentType(t *testing.T) {
	m := streams.FailingMarshalerNoop{}
	assert.Equal(t, m.ContentType(), "")
}

func TestFailingMarshaler_Marshal(t *testing.T) {
	m := streams.FailingMarshalerNoop{}
	bytesWritten, err := m.Marshal("", nil)
	assert.Nil(t, bytesWritten)
	assert.Error(t, err)
}

func TestFailingMarshaler_Unmarshal(t *testing.T) {
	m := streams.FailingMarshalerNoop{}
	err := m.Unmarshal("", nil, nil)
	assert.Error(t, err)
}

func TestJSONMarshaler_Marshal(t *testing.T) {
	msg := fooMessage{Foo: "foo"}
	m := streams.JSONMarshaler{}
	data, err := m.Marshal("", msg)
	assert.NotNil(t, data)
	assert.NoError(t, err)
}

func TestJSONMarshaler_Unmarshal(t *testing.T) {
	msg := fooMessage{Foo: "foo"}
	m := streams.JSONMarshaler{}
	data, err := m.Marshal("", msg)
	assert.NoError(t, err)
	assert.NotNil(t, data)
	msgRef := fooMessage{}
	err = m.Unmarshal("", data, &msgRef)
	assert.NoError(t, err)
	assert.NotEmpty(t, msgRef)
}

func TestJSONMarshaler_ContentType(t *testing.T) {
	m := streams.JSONMarshaler{}
	assert.Equal(t, "application/json", m.ContentType())
}

func BenchmarkJSONMarshaler_Marshal(b *testing.B) {
	msg := fooMessage{Foo: "foo"}
	m := streams.JSONMarshaler{}
	for i := 0; i < b.N; i++ {
		b.ReportAllocs()
		_, _ = m.Marshal("", msg)
	}
}

func BenchmarkJSONMarshaler_Unmarshal(b *testing.B) {
	msg := fooMessage{Foo: "foo"}
	m := streams.JSONMarshaler{}
	data, _ := m.Marshal("", msg)
	ref := &fooMessage{}
	for i := 0; i < b.N; i++ {
		b.ReportAllocs()
		_ = m.Unmarshal("", data, ref)
	}
}

func TestAvroMarshaler_Marshal(t *testing.T) {
	msg := fooMessage{Foo: "foo"}
	m := streams.NewAvroMarshaler()
	data, err := m.Marshal("", msg)
	assert.Nil(t, data)
	assert.Error(t, err)

	m.HashingFactory = func() hash.Hash64 {
		return hashing64AlgorithmFailingNoop{}
	}

	data, err = m.Marshal(`{
		"type": "record",
		"name": "fooMessage",
		"namespace": "org.ncorp.avro",
		"fields" : [
			{"name": "foo", "type": "string"}
		]
	}`, msg)
	assert.Nil(t, data)
	assert.ErrorIs(t, err, hashing64GenericError)

	m.HashingFactory = streams.DefaultHashing64AlgorithmFactory
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

	// test caching
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
	m := streams.NewAvroMarshaler()
	data, err := m.Marshal(def, msg)
	assert.NotNil(t, data)
	assert.NoError(t, err)
	msgRef := fooMessage{}

	err = m.Unmarshal("", data, &msgRef)
	assert.Error(t, err)
	assert.Empty(t, msgRef)

	m.HashingFactory = func() hash.Hash64 {
		return hashing64AlgorithmFailingNoop{}
	}
	err = m.Unmarshal(def, data, &msgRef)
	assert.ErrorIs(t, err, hashing64GenericError)
	assert.Empty(t, msgRef)

	m.HashingFactory = streams.DefaultHashing64AlgorithmFactory
	err = m.Unmarshal(def, data, &msgRef)
	assert.NoError(t, err)
	assert.NotEmpty(t, msgRef)
}

func TestAvroMarshaler_ContentType(t *testing.T) {
	m := streams.AvroMarshaler{}
	assert.Equal(t, "application/avro", m.ContentType())
}

func BenchmarkAvroMarshaler_Marshal(b *testing.B) {
	msg := fooMessage{Foo: "foo"}
	m := streams.NewAvroMarshaler()

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
	m := streams.NewAvroMarshaler()
	data, _ := m.Marshal(def, msg)

	ref := &fooMessage{}
	for i := 0; i < b.N; i++ {
		b.ReportAllocs()
		_ = m.Unmarshal(def, data, ref)
	}
}

func TestProtocolBuffersMarshaler_ContentType(t *testing.T) {
	assert.Equal(t, "application/octet-stream", streams.ProtocolBuffersMarshaler{}.ContentType())
}

func TestProtocolBuffersMarshaler_Marshal(t *testing.T) {
	var m streams.Marshaler
	m = streams.ProtocolBuffersMarshaler{}
	data, err := m.Marshal("", fooMessage{})
	assert.Error(t, err)
	assert.Nil(t, data)

	// not pointer, will fail
	data, err = m.Marshal("", examplepb.Person{})
	assert.ErrorIs(t, err, streams.ErrInvalidProtocolBufferFormat)
	assert.Nil(t, data)

	data, err = m.Marshal("", &examplepb.Person{})
	assert.NoError(t, err)
	assert.NotNil(t, data)
}

func TestProtocolBuffersMarshaler_Unmarshal(t *testing.T) {
	var m streams.Marshaler
	m = streams.ProtocolBuffersMarshaler{}
	basePerson := &examplepb.Person{
		Name:  "Foo",
		Id:    123,
		Email: "foo@example.com",
		Phones: []*examplepb.Person_PhoneNumber{
			{
				Number: "017865642",
				Type:   2,
			},
		},
		LastUpdated: &timestamppb.Timestamp{
			Seconds: time.Now().Unix(),
			Nanos:   int32(time.Now().UnixMilli()),
		},
	}

	data, err := m.Marshal("", basePerson)
	assert.NoError(t, err)
	assert.NotNil(t, data)

	err = m.Unmarshal("", data, "foobar")
	assert.ErrorIs(t, err, streams.ErrInvalidProtocolBufferFormat)

	decodedPerson := &examplepb.Person{}
	err = m.Unmarshal("", data, decodedPerson)
	assert.NoError(t, err)
	assert.Equal(t, basePerson.Id, decodedPerson.Id)
	assert.Equal(t, basePerson.Name, decodedPerson.Name)
	assert.Equal(t, basePerson.Email, decodedPerson.Email)
	assert.Equal(t, basePerson.LastUpdated.String(), decodedPerson.LastUpdated.String())
	assert.Equal(t, len(basePerson.Phones), len(decodedPerson.Phones))
	assert.Equal(t, basePerson.Phones[0].Type, decodedPerson.Phones[0].Type)
	assert.Equal(t, basePerson.Phones[0].Number, decodedPerson.Phones[0].Number)
}

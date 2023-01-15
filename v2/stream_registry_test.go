package streams_test

import (
	"testing"

	"github.com/neutrinocorp/streams/v2"
)

func TestStreamRegistry_Get(t *testing.T) {
	reg := streams.NewStreamRegistry()
	topic := "ncorp.places.orders.*"
	msg := fakeMessage{}
	reg.Set(topic, msg, streams.WithSchemaURL("foo.com"))
	meta, _ := reg.Get(topic)
	t.Logf("stream_name:%s,stream_schema_typeof:%s,stream_url:%s", meta.Name, meta.TypeOf, meta.SchemaURL)
	meta, _ = reg.GetByType(msg)
	t.Logf("stream_name:%s,stream_schema_typeof:%s", meta.Name, meta.TypeOf)
}

package streams_test

import (
	"testing"

	"github.com/neutrinocorp/streams/v2"
)

func TestStreamRegistry_Get(t *testing.T) {
	reg := streams.NewStreamRegistry()
	topic := "ncorp.places.orders.*"
	msg := fakeMessage{}
	reg.Set(topic, streams.StreamMetadata{}, msg)
	meta, _ := reg.Get(topic)
	t.Logf("stream_name:%s,stream_schema_typeof:%s", meta.Name, meta.TypeOf)
	meta, _ = reg.GetByType(msg)
	t.Logf("stream_name:%s,stream_schema_typeof:%s", meta.Name, meta.TypeOf)
}

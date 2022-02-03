package streamhub_test

import (
	"reflect"
	"strconv"
	"testing"

	"github.com/neutrinocorp/streamhub"
	"github.com/stretchr/testify/assert"
)

type fooMessage struct {
	Foo string `json:"foo" avro:"foo"`
}

var streamRegistrySetSuite = []struct {
	InMsg  interface{}
	InMeta streamhub.StreamMetadata
	Exp    streamhub.StreamMetadata
	Err    error
}{
	{
		InMsg:  fooMessage{},
		InMeta: streamhub.StreamMetadata{},
		Exp: streamhub.StreamMetadata{
			GoType: reflect.TypeOf(fooMessage{}),
		},
		Err: nil,
	},
	{
		InMsg: fooMessage{},
		InMeta: streamhub.StreamMetadata{
			Stream:               "foo-stream",
			SchemaDefinitionName: "",
		},
		Exp: streamhub.StreamMetadata{
			Stream: "foo-stream",
			GoType: reflect.TypeOf(fooMessage{}),
		},
		Err: nil,
	},
}

func TestStreamRegistry_Set(t *testing.T) {
	for _, tt := range streamRegistrySetSuite {
		t.Run("", func(t *testing.T) {
			registry := streamhub.StreamRegistry{}
			registry.Set(tt.InMsg, tt.InMeta)
			exp, err := registry.Get(tt.InMsg)
			assert.EqualValues(t, tt.Exp, exp)
			assert.Equal(t, tt.Err, err)
		})
	}
}

var streamRegistrySetByStringSuite = []struct {
	InMsgKey string
	InMeta   streamhub.StreamMetadata
	Err      error
}{
	{
		InMsgKey: "",
		InMeta:   streamhub.StreamMetadata{},
		Err:      nil,
	},
	{
		InMsgKey: "foo",
		InMeta: streamhub.StreamMetadata{
			Stream:               "foo-stream",
			SchemaDefinitionName: "",
		},
		Err: nil,
	},
	{
		InMsgKey: "bar",
		InMeta: streamhub.StreamMetadata{
			Stream:               "foo-stream",
			SchemaDefinitionName: "",
		},
		Err: nil,
	},
}

func TestStreamRegistry_SetByString(t *testing.T) {
	registry := streamhub.StreamRegistry{}
	for _, tt := range streamRegistrySetByStringSuite {
		t.Run("", func(t *testing.T) {
			registry.SetByString(tt.InMsgKey, tt.InMeta)
			exp, err := registry.GetByString(tt.InMsgKey)
			assert.Equal(t, tt.InMeta, exp)
			assert.Equal(t, tt.Err, err)
		})
	}
}

var streamRegistryGetByStringSuite = []struct {
	In  string
	Err error
}{
	{
		In:  "",
		Err: streamhub.ErrMissingStream,
	},
	{
		In:  "bar",
		Err: streamhub.ErrMissingStream,
	},
	{
		In:  "streamhub_test.fooMessage",
		Err: nil,
	},
}

func TestStreamRegistry_GetByString(t *testing.T) {
	registry := streamhub.StreamRegistry{}
	registry.Set(fooMessage{}, streamhub.StreamMetadata{
		Stream:               "foo-stream",
		SchemaDefinitionName: "./streams-schemas/foo.avsc",
	})
	for _, tt := range streamRegistryGetByStringSuite {
		t.Run("", func(t *testing.T) {
			_, err := registry.GetByString(tt.In)
			assert.Equal(t, tt.Err, err)
		})
	}
}

func TestStreamRegistry_GetByStreamNameReflection(t *testing.T) {
	registry := streamhub.StreamRegistry{}
	registry.Set(fooMessage{}, streamhub.StreamMetadata{
		Stream: "foo-stream",
	})
	_, err := registry.GetByStreamName("foo-stream")
	assert.Nil(t, err)
}

var streamRegistryGetByStreamSuite = []struct {
	In  string
	Err error
}{
	{
		In:  "",
		Err: streamhub.ErrMissingStream,
	},
	{
		In:  "bar",
		Err: streamhub.ErrMissingStream,
	},
	{
		In:  reflect.TypeOf(fooMessage{}).String(),
		Err: streamhub.ErrMissingStream,
	},
	{
		In:  "foo-stream",
		Err: nil,
	},
}

func TestStreamRegistry_GetByStreamName(t *testing.T) {
	registry := streamhub.StreamRegistry{}
	registry.SetByString("foo-stream", streamhub.StreamMetadata{
		Stream: "foo-stream",
	})
	for _, tt := range streamRegistryGetByStreamSuite {
		t.Run("", func(t *testing.T) {
			_, err := registry.GetByStreamName(tt.In)
			assert.Equal(t, tt.Err, err)
		})
	}
}

func BenchmarkStreamRegistry_Set(b *testing.B) {
	registry := streamhub.StreamRegistry{}
	for i := 0; i < b.N; i++ {
		b.ReportAllocs()
		registry.Set(fooMessage{}, streamhub.StreamMetadata{
			Stream:               "foo-stream",
			SchemaDefinitionName: "./streams-schemas/foo.avsc",
		})
	}
}

func BenchmarkStreamRegistry_SetByString(b *testing.B) {
	registry := streamhub.StreamRegistry{}
	for i := 0; i < b.N; i++ {
		key := "foo" + strconv.Itoa(i)
		b.ReportAllocs()
		registry.SetByString(key, streamhub.StreamMetadata{
			Stream:               "foo-stream",
			SchemaDefinitionName: "./streams-schemas/foo.avsc",
		})
	}
}

func BenchmarkStreamRegistry_Get(b *testing.B) {
	registry := streamhub.StreamRegistry{}
	registry.Set(fooMessage{}, streamhub.StreamMetadata{
		Stream:               "foo-stream",
		SchemaDefinitionName: "./streams-schemas/foo.avsc",
	})

	for i := 0; i < b.N; i++ {
		b.ReportAllocs()
		_, _ = registry.Get(fooMessage{})
	}
}

func BenchmarkStreamRegistry_GetByString(b *testing.B) {
	registry := streamhub.StreamRegistry{}
	registry.SetByString("bar-stream", streamhub.StreamMetadata{
		Stream:               "bar-stream",
		SchemaDefinitionName: "./streams-schemas/bar.avsc",
	})
	registry.SetByString("baz-stream", streamhub.StreamMetadata{
		Stream:               "baz-stream",
		SchemaDefinitionName: "./streams-schemas/baz.avsc",
	})
	registry.SetByString("barbaz-stream", streamhub.StreamMetadata{
		Stream:               "barbaz-stream",
		SchemaDefinitionName: "./streams-schemas/barbaz.avsc",
	})
	registry.Set(fooMessage{}, streamhub.StreamMetadata{
		Stream:               "foo-stream",
		SchemaDefinitionName: "./streams-schemas/foo.avsc",
	})
	for i := 0; i < b.N; i++ {
		b.ReportAllocs()
		_, _ = registry.GetByString("foo-stream")
	}
}

func BenchmarkStreamRegistry_GetByStreamNameReflection(b *testing.B) {
	registry := streamhub.StreamRegistry{}
	registry.SetByString("bar-stream", streamhub.StreamMetadata{
		Stream:               "bar-stream",
		SchemaDefinitionName: "./streams-schemas/bar.avsc",
	})
	registry.SetByString("baz-stream", streamhub.StreamMetadata{
		Stream:               "baz-stream",
		SchemaDefinitionName: "./streams-schemas/baz.avsc",
	})
	registry.SetByString("barbaz-stream", streamhub.StreamMetadata{
		Stream:               "barbaz-stream",
		SchemaDefinitionName: "./streams-schemas/barbaz.avsc",
	})
	registry.Set(fooMessage{}, streamhub.StreamMetadata{
		Stream:               "foo-stream",
		SchemaDefinitionName: "./streams-schemas/foo.avsc",
	})

	for i := 0; i < b.N; i++ {
		b.ReportAllocs()
		_, _ = registry.GetByStreamName("foo-stream")
	}
}

func BenchmarkStreamRegistry_GetByStreamName(b *testing.B) {
	registry := streamhub.StreamRegistry{}
	registry.SetByString("bar-stream", streamhub.StreamMetadata{
		Stream:               "bar-stream",
		SchemaDefinitionName: "./streams-schemas/bar.avsc",
	})
	registry.SetByString("baz-stream", streamhub.StreamMetadata{
		Stream:               "baz-stream",
		SchemaDefinitionName: "./streams-schemas/baz.avsc",
	})
	registry.SetByString("barbaz-stream", streamhub.StreamMetadata{
		Stream:               "barbaz-stream",
		SchemaDefinitionName: "./streams-schemas/barbaz.avsc",
	})
	registry.SetByString("foo-stream", streamhub.StreamMetadata{
		Stream:               "foo-stream",
		SchemaDefinitionName: "./streams-schemas/foo.avsc",
	})

	for i := 0; i < b.N; i++ {
		b.ReportAllocs()
		_, _ = registry.GetByStreamName("foo-stream")
	}
}

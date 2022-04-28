package streams_test

import (
	"strconv"
	"testing"

	"github.com/modern-go/reflect2"
	"github.com/neutrinocorp/streams"
	"github.com/stretchr/testify/assert"
)

type fooMessage struct {
	Foo string `json:"foo" avro:"foo"`
}

var streamRegistrySetSuite = []struct {
	InMsg  interface{}
	InMeta streams.StreamMetadata
	Exp    streams.StreamMetadata
	Err    error
}{
	{
		InMsg:  fooMessage{},
		InMeta: streams.StreamMetadata{},
		Exp: streams.StreamMetadata{
			GoType: reflect2.TypeOf(fooMessage{}),
		},
		Err: nil,
	},
	{
		InMsg: fooMessage{},
		InMeta: streams.StreamMetadata{
			Stream:               "foo-stream",
			SchemaDefinitionName: "",
		},
		Exp: streams.StreamMetadata{
			Stream: "foo-stream",
			GoType: reflect2.TypeOf(fooMessage{}),
		},
		Err: nil,
	},
}

func TestStreamRegistry_Set(t *testing.T) {
	for _, tt := range streamRegistrySetSuite {
		t.Run("", func(t *testing.T) {
			registry := streams.NewStreamRegistry()
			registry.Set(tt.InMsg, tt.InMeta)
			exp, err := registry.Get(tt.InMsg)
			assert.EqualValues(t, tt.Exp, exp)
			assert.Equal(t, tt.Err, err)
		})
	}
}

var streamRegistrySetByStringSuite = []struct {
	InMsgKey string
	InMeta   streams.StreamMetadata
	Err      error
}{
	{
		InMsgKey: "",
		InMeta:   streams.StreamMetadata{},
		Err:      nil,
	},
	{
		InMsgKey: "foo",
		InMeta: streams.StreamMetadata{
			Stream:               "foo-stream",
			SchemaDefinitionName: "",
		},
		Err: nil,
	},
	{
		InMsgKey: "bar",
		InMeta: streams.StreamMetadata{
			Stream:               "foo-stream",
			SchemaDefinitionName: "",
		},
		Err: nil,
	},
}

func TestStreamRegistry_SetByString(t *testing.T) {
	registry := streams.NewStreamRegistry()
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
		Err: streams.ErrMissingStream,
	},
	{
		In:  "bar",
		Err: streams.ErrMissingStream,
	},
	{
		In:  "streams_test.fooMessage",
		Err: nil,
	},
}

func TestStreamRegistry_GetByString(t *testing.T) {
	registry := streams.NewStreamRegistry()
	registry.Set(fooMessage{}, streams.StreamMetadata{
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
	registry := streams.NewStreamRegistry()
	registry.Set(fooMessage{}, streams.StreamMetadata{
		Stream: "foo-stream",
	})
	_, err := registry.GetByStreamName("foo-stream")
	assert.Nil(t, err)
}

var streamRegistryGetByStreamSuite = []struct {
	Name string
	In   string
	Err  error
}{
	{
		Name: "Empty",
		In:   "",
		Err:  streams.ErrMissingStream,
	},
	{
		Name: "Missing stream",
		In:   "bar",
		Err:  streams.ErrMissingStream,
	},
	{
		Name: "Reflect package name",
		In:   reflect2.TypeOf(fooMessage{}).String(),
		Err:  streams.ErrMissingStream,
	},
	{
		Name: "Search by value",
		In:   "foo-stream",
		Err:  nil,
	},
	{
		Name: "Search by key",
		In:   "foo-bar-stream",
		Err:  nil,
	},
}

func TestStreamRegistry_GetByStreamName(t *testing.T) {
	registry := streams.NewStreamRegistry()
	registry.SetByString("foo-bar-baz", streams.StreamMetadata{
		Stream: "foo-stream",
	})
	registry.SetByString("foo-bar-stream", streams.StreamMetadata{
		Stream: "foo-bar-stream-v2",
	})
	for _, tt := range streamRegistryGetByStreamSuite {
		t.Run(tt.Name, func(t *testing.T) {
			_, err := registry.GetByStreamName(tt.In)
			assert.Equal(t, tt.Err, err)
		})
	}
}

func BenchmarkStreamRegistry_Set(b *testing.B) {
	registry := streams.NewStreamRegistry()
	for i := 0; i < b.N; i++ {
		b.ReportAllocs()
		registry.Set(fooMessage{}, streams.StreamMetadata{
			Stream:               "foo-stream",
			SchemaDefinitionName: "./streams-schemas/foo.avsc",
		})
	}
}

func BenchmarkStreamRegistry_SetByString(b *testing.B) {
	registry := streams.NewStreamRegistry()
	for i := 0; i < b.N; i++ {
		key := "foo" + strconv.Itoa(i)
		b.ReportAllocs()
		registry.SetByString(key, streams.StreamMetadata{
			Stream:               "foo-stream",
			SchemaDefinitionName: "./streams-schemas/foo.avsc",
		})
	}
}

func BenchmarkStreamRegistry_Get(b *testing.B) {
	registry := streams.NewStreamRegistry()
	registry.Set(fooMessage{}, streams.StreamMetadata{
		Stream:               "foo-stream",
		SchemaDefinitionName: "./streams-schemas/foo.avsc",
	})

	for i := 0; i < b.N; i++ {
		b.ReportAllocs()
		_, _ = registry.Get(fooMessage{})
	}
}

func BenchmarkStreamRegistry_GetByString(b *testing.B) {
	registry := streams.NewStreamRegistry()
	registry.SetByString("bar-stream", streams.StreamMetadata{
		Stream:               "bar-stream",
		SchemaDefinitionName: "./streams-schemas/bar.avsc",
	})
	registry.SetByString("baz-stream", streams.StreamMetadata{
		Stream:               "baz-stream",
		SchemaDefinitionName: "./streams-schemas/baz.avsc",
	})
	registry.SetByString("barbaz-stream", streams.StreamMetadata{
		Stream:               "barbaz-stream",
		SchemaDefinitionName: "./streams-schemas/barbaz.avsc",
	})
	registry.Set(fooMessage{}, streams.StreamMetadata{
		Stream:               "foo-stream",
		SchemaDefinitionName: "./streams-schemas/foo.avsc",
	})
	for i := 0; i < b.N; i++ {
		b.ReportAllocs()
		_, _ = registry.GetByString("foo-stream")
	}
}

func BenchmarkStreamRegistry_GetByStreamNameReflection(b *testing.B) {
	registry := streams.NewStreamRegistry()
	registry.Set(fooMessage{}, streams.StreamMetadata{
		Stream:               "bar-stream",
		SchemaDefinitionName: "./streams-schemas/bar.avsc",
	})
	registry.Set(fooMessage{}, streams.StreamMetadata{
		Stream:               "baz-stream",
		SchemaDefinitionName: "./streams-schemas/baz.avsc",
	})
	registry.Set(fooMessage{}, streams.StreamMetadata{
		Stream:               "foo_bar-stream",
		SchemaDefinitionName: "./streams-schemas/foo_bar.avsc",
	})
	registry.Set(fooMessage{}, streams.StreamMetadata{
		Stream:               "foo-stream",
		SchemaDefinitionName: "./streams-schemas/foo.avsc",
	})

	for i := 0; i < b.N; i++ {
		b.ReportAllocs()
		_, _ = registry.GetByStreamName("foo-stream")
	}
}

func BenchmarkStreamRegistry_GetByStreamName(b *testing.B) {
	registry := streams.NewStreamRegistry()
	registry.SetByString("bar-stream", streams.StreamMetadata{
		Stream:               "bar-stream",
		SchemaDefinitionName: "./streams-schemas/bar.avsc",
	})
	registry.SetByString("baz-stream", streams.StreamMetadata{
		Stream:               "baz-stream",
		SchemaDefinitionName: "./streams-schemas/baz.avsc",
	})
	registry.SetByString("barbaz-stream", streams.StreamMetadata{
		Stream:               "barbaz-stream",
		SchemaDefinitionName: "./streams-schemas/barbaz.avsc",
	})
	registry.SetByString("foo-stream", streams.StreamMetadata{
		Stream:               "foo-stream",
		SchemaDefinitionName: "./streams-schemas/foo.avsc",
	})

	for i := 0; i < b.N; i++ {
		b.ReportAllocs()
		_, _ = registry.GetByStreamName("foo-stream")
	}
}

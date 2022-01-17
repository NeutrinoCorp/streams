package streamhub_test

import (
	"context"
	"strconv"
	"testing"

	"github.com/neutrinocorp/streamhub"
	"github.com/stretchr/testify/assert"
)

func TestHub_Publish(t *testing.T) {
	hub := streamhub.NewHub(streamhub.WithSchemaRegistry(streamhub.NoopSchemaRegistry{}),
		streamhub.WithPublisher(streamhub.NoopPublisher))
	hub.PublisherFunc = nil
	ctx := context.Background()
	err := hub.Publish(ctx, fooMessage{
		Foo: "foo",
	})
	assert.ErrorIs(t, err, streamhub.ErrMissingStream)

	hub.RegisterStream(fooMessage{}, streamhub.StreamMetadata{
		Stream:               "foo-stream",
		SchemaDefinitionName: "",
		SchemaVersion:        0,
	})
	err = hub.Publish(ctx, fooMessage{
		Foo: "foo",
	})
	assert.NoError(t, err)
}

func TestHub_Publish_Func(t *testing.T) {
	hub := streamhub.NewHub(streamhub.WithSchemaRegistry(streamhub.NoopSchemaRegistry{}))
	ctx := context.Background()
	err := hub.Publish(ctx, fooMessage{
		Foo: "foo",
	})
	assert.ErrorIs(t, err, streamhub.ErrMissingStream)

	hub.RegisterStream(fooMessage{}, streamhub.StreamMetadata{
		Stream:               "foo-stream",
		SchemaDefinitionName: "",
		SchemaVersion:        0,
	})
	err = hub.Publish(ctx, fooMessage{
		Foo: "foo",
	})
	assert.NoError(t, err)
}

func TestHub_Publish_With_Bad_Marshaling(t *testing.T) {
	hub := streamhub.NewHub(streamhub.WithSchemaRegistry(streamhub.NoopSchemaRegistry{}),
		streamhub.WithMarshaler(failingFakeMarshaler{}))
	ctx := context.Background()

	hub.RegisterStream(fooMessage{}, streamhub.StreamMetadata{
		Stream:               "foo-stream",
		SchemaDefinitionName: "",
		SchemaVersion:        0,
	})
	err := hub.Publish(ctx, fooMessage{
		Foo: "foo",
	})
	assert.Error(t, err)
}

func TestHub_Publish_With_Bad_ID_Factory(t *testing.T) {
	hub := streamhub.NewHub(streamhub.WithSchemaRegistry(streamhub.NoopSchemaRegistry{}),
		streamhub.WithIDFactory(failingFakeIDFactory))
	ctx := context.Background()

	hub.RegisterStream(fooMessage{}, streamhub.StreamMetadata{
		Stream:               "foo-stream",
		SchemaDefinitionName: "",
		SchemaVersion:        0,
	})
	err := hub.Publish(ctx, fooMessage{
		Foo: "foo",
	})
	assert.Error(t, err)
}

func TestHub_PublishRawMessage(t *testing.T) {
	hub := streamhub.NewHub()
	ctx := context.Background()

	err := hub.PublishRawMessage(ctx, streamhub.NewMessage(streamhub.NewMessageArgs{
		SchemaVersion:        9,
		Data:                 []byte("hello there"),
		ID:                   "1",
		Source:               "",
		Stream:               "bar-stream",
		SchemaDefinitionName: "",
		ContentType:          "",
	}))
	assert.NoError(t, err)
}

func TestHub_PublishInMemorySchemaRegistry(t *testing.T) {
	r := streamhub.InMemorySchemaRegistry{}
	hub := streamhub.NewHub(streamhub.WithSchemaRegistry(r))
	ctx := context.Background()
	hub.RegisterStream(fooMessage{}, streamhub.StreamMetadata{
		Stream:               "foo-stream",
		SchemaDefinitionName: "foo",
		SchemaVersion:        7,
	})
	err := hub.Publish(ctx, fooMessage{
		Foo: "foo",
	})
	assert.ErrorIs(t, err, streamhub.ErrMissingSchemaDefinition)

	r.RegisterDefinition("foo", "sample_foo_format", 7)
	err = hub.Publish(ctx, fooMessage{
		Foo: "foo",
	})
	assert.NoError(t, err)
}

func TestHub_PublishByMessageKey(t *testing.T) {
	hub := streamhub.NewHub(streamhub.WithSchemaRegistry(streamhub.NoopSchemaRegistry{}))
	ctx := context.Background()
	err := hub.PublishByMessageKey(ctx, "foo", fooMessage{
		Foo: "foo",
	})
	assert.ErrorIs(t, err, streamhub.ErrMissingStream)

	hub.RegisterStreamByString("foo_custom", streamhub.StreamMetadata{
		Stream:               "foo-stream",
		SchemaDefinitionName: "",
		SchemaVersion:        0,
	})
	err = hub.PublishByMessageKey(ctx, "foo_custom", fooMessage{
		Foo: "custom",
	})
	assert.NoError(t, err)
}

func BenchmarkHub_RegisterStream(b *testing.B) {
	hub := streamhub.NewHub()
	for i := 0; i < b.N; i++ {
		b.ReportAllocs()
		hub.RegisterStream(fooMessage{}, streamhub.StreamMetadata{
			Stream:               "foo-stream",
			SchemaDefinitionName: "foo_stream",
			SchemaVersion:        8,
		})
	}
}

func BenchmarkHub_RegisterStreamByString(b *testing.B) {
	hub := streamhub.NewHub()
	for i := 0; i < b.N; i++ {
		b.ReportAllocs()
		hub.RegisterStreamByString("fooMessage"+strconv.Itoa(i), streamhub.StreamMetadata{
			Stream:               "foo-stream",
			SchemaDefinitionName: "foo_stream",
			SchemaVersion:        8,
		})
	}
}

func BenchmarkHub_Publish(b *testing.B) {
	hub := streamhub.NewHub()
	hub.RegisterStream(fooMessage{}, streamhub.StreamMetadata{
		Stream: "foo-stream",
	})
	msg := fooMessage{
		Foo: "1",
	}

	for i := 0; i < b.N; i++ {
		b.ReportAllocs()
		err := hub.Publish(context.Background(), msg)
		if err != nil {
			panic(err)
		}
	}
}

func BenchmarkHub_Publish_With_Schema_Registry(b *testing.B) {
	r := streamhub.InMemorySchemaRegistry{}
	r.RegisterDefinition("foo-stream", "elver", 0)
	hub := streamhub.NewHub(streamhub.WithSchemaRegistry(r))
	hub.RegisterStream(fooMessage{}, streamhub.StreamMetadata{
		Stream:               "foo-stream",
		SchemaDefinitionName: "foo-stream",
		SchemaVersion:        0,
	})
	msg := fooMessage{
		Foo: "1",
	}

	for i := 0; i < b.N; i++ {
		b.ReportAllocs()
		err := hub.Publish(context.Background(), msg)
		if err != nil {
			panic(err)
		}
	}
}

func BenchmarkHub_PublishByMessageKey(b *testing.B) {
	hub := streamhub.NewHub()
	hub.RegisterStreamByString("fooMessage", streamhub.StreamMetadata{
		Stream: "foo-stream",
	})
	msg := fooMessage{
		Foo: "1",
	}

	for i := 0; i < b.N; i++ {
		b.ReportAllocs()
		err := hub.PublishByMessageKey(context.Background(), "fooMessage", msg)
		if err != nil {
			panic(err)
		}
	}
}

func BenchmarkHub_PublishRawMessage(b *testing.B) {
	hub := streamhub.NewHub()
	args := streamhub.NewMessageArgs{
		SchemaVersion:        9,
		Data:                 []byte("hello there"),
		ID:                   "1",
		Source:               "",
		Stream:               "bar-stream",
		SchemaDefinitionName: "",
		ContentType:          "",
	}

	for i := 0; i < b.N; i++ {
		b.ReportAllocs()
		err := hub.PublishRawMessage(context.Background(), streamhub.NewMessage(args))
		if err != nil {
			panic(err)
		}
	}
}

func BenchmarkHub_PublishPreBuildRawMessage(b *testing.B) {
	hub := streamhub.NewHub()
	msg := streamhub.NewMessage(streamhub.NewMessageArgs{ // 2 allocs
		SchemaVersion:        9,
		Data:                 []byte("hello there"),
		ID:                   "1",
		Source:               "",
		Stream:               "bar-stream",
		SchemaDefinitionName: "",
		ContentType:          "",
	})
	for i := 0; i < b.N; i++ {
		b.ReportAllocs()
		err := hub.PublishRawMessage(context.Background(), msg)
		if err != nil {
			panic(err)
		}
	}
}

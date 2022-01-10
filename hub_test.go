package streamhub_test

import (
	"context"
	"strconv"
	"testing"

	"github.com/neutrinocorp/streamhub"
	"github.com/stretchr/testify/assert"
)

func TestHub_Publish(t *testing.T) {
	hub := streamhub.NewHub(streamhub.WithSchemaRegistry(streamhub.NoopSchemaRegistry{}))
	ctx := context.Background()
	err := hub.Publish(ctx, fooMessage{
		Foo: "foo",
	})
	assert.ErrorIs(t, err, streamhub.ErrMissingStream)

	hub.RegisterStream(fooMessage{}, streamhub.StreamMetadata{
		Stream:           "foo-stream",
		SchemaDefinition: "",
		SchemaVersion:    0,
	})
	err = hub.Publish(ctx, fooMessage{
		Foo: "foo",
	})
	assert.NoError(t, err)
}

func TestHub_PublishRawMessage(t *testing.T) {
	hub := streamhub.NewHub()
	ctx := context.Background()

	err := hub.PublishRawMessage(ctx, streamhub.NewMessage(streamhub.NewMessageArgs{
		SchemaVersion:    9,
		Data:             []byte("hello there"),
		ID:               "1",
		Source:           "",
		Stream:           "bar-stream",
		SchemaDefinition: "",
		ContentType:      "",
	}))
	assert.NoError(t, err)
}

func TestHub_PublishInMemorySchemaRegistry(t *testing.T) {
	r := streamhub.InMemorySchemaRegistry{}
	hub := streamhub.NewHub(streamhub.WithSchemaRegistry(r))
	ctx := context.Background()
	hub.RegisterStream(fooMessage{}, streamhub.StreamMetadata{
		Stream:           "foo-stream",
		SchemaDefinition: "foo",
		SchemaVersion:    7,
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
		Stream:           "foo-stream",
		SchemaDefinition: "",
		SchemaVersion:    0,
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
		hub.StreamRegistry.Set(fooMessage{}, streamhub.StreamMetadata{
			Stream:           "foo-stream",
			SchemaDefinition: "foo_stream",
			SchemaVersion:    8,
		})
	}
}

func BenchmarkHub_RegisterStreamByString(b *testing.B) {
	hub := streamhub.NewHub()
	for i := 0; i < b.N; i++ {
		b.ReportAllocs()
		hub.StreamRegistry.SetByString("fooMessage"+strconv.Itoa(i), streamhub.StreamMetadata{
			Stream:           "foo-stream",
			SchemaDefinition: "foo_stream",
			SchemaVersion:    8,
		})
	}
}

func BenchmarkHub_Publish(b *testing.B) {
	hub := streamhub.NewHub()
	hub.StreamRegistry.Set(fooMessage{}, streamhub.StreamMetadata{
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

func BenchmarkHub_PublishByMessageKey(b *testing.B) {
	hub := streamhub.NewHub()
	hub.StreamRegistry.SetByString("fooMessage", streamhub.StreamMetadata{
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
		SchemaVersion:    9,
		Data:             []byte("hello there"),
		ID:               "1",
		Source:           "",
		Stream:           "bar-stream",
		SchemaDefinition: "",
		ContentType:      "",
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
		SchemaVersion:    9,
		Data:             []byte("hello there"),
		ID:               "1",
		Source:           "",
		Stream:           "bar-stream",
		SchemaDefinition: "",
		ContentType:      "",
	})
	for i := 0; i < b.N; i++ {
		b.ReportAllocs()
		err := hub.PublishRawMessage(context.Background(), msg)
		if err != nil {
			panic(err)
		}
	}
}

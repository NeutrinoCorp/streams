package streamhub_test

import (
	"context"
	"errors"
	"runtime"
	"strconv"
	"testing"
	"time"

	"github.com/neutrinocorp/streamhub"
	"github.com/stretchr/testify/assert"
)

func TestHub_Publish(t *testing.T) {
	hub := streamhub.NewHub(streamhub.WithSchemaRegistry(streamhub.NoopSchemaRegistry{}))
	hub.Publisher = nil
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
	assert.ErrorIs(t, err, streamhub.ErrMissingPublisherDriver)

	hub.Publisher = streamhub.NoopPublisher
	err = hub.Publish(ctx, fooMessage{
		Foo: "foo",
	})
	assert.NoError(t, err)
}

func TestHub_PublishBatch(t *testing.T) {
	hub := streamhub.NewHub(streamhub.WithSchemaRegistry(streamhub.NoopSchemaRegistry{}))
	hub.Publisher = nil
	ctx := context.Background()
	err := hub.PublishBatch(ctx, fooMessage{
		Foo: "foo",
	})
	assert.ErrorIs(t, err, streamhub.ErrMissingStream)

	hub.RegisterStream(fooMessage{}, streamhub.StreamMetadata{
		Stream:               "foo-stream",
		SchemaDefinitionName: "",
		SchemaVersion:        0,
	})
	err = hub.PublishBatch(ctx, fooMessage{
		Foo: "foo",
	})
	assert.ErrorIs(t, err, streamhub.ErrMissingPublisherDriver)

	hub.Publisher = streamhub.NoopPublisher
	hub.IDFactory = failingFakeIDFactory
	err = hub.PublishBatch(ctx, fooMessage{
		Foo: "foo",
	})
	assert.EqualValues(t, errors.New("generic id factory error"), err)

	hub.IDFactory = streamhub.RandInt64Factory
	err = hub.PublishBatch(ctx, fooMessage{
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

type fooEvent struct {
	Foo string `json:"foo"`
	Bar string `json:"bar"`
}

var _ streamhub.Event = fooEvent{}

func (f fooEvent) Subject() string {
	return f.Bar
}

func TestHub_Publish_Event(t *testing.T) {
	hub := streamhub.NewHub(streamhub.WithSchemaRegistry(streamhub.NoopSchemaRegistry{}))
	ctx := context.Background()
	hub.RegisterStream(fooMessage{}, streamhub.StreamMetadata{
		Stream:               "foo-stream",
		SchemaDefinitionName: "",
		SchemaVersion:        0,
	})
	hub.RegisterStream(fooEvent{}, streamhub.StreamMetadata{
		Stream:               "foo-stream",
		SchemaDefinitionName: "",
		SchemaVersion:        0,
	})

	noopPublisher := &publisherNoopHook{}
	hub.Publisher = noopPublisher
	noopPublisher.onPublish = func(ctx context.Context, message streamhub.Message) error {
		assert.Empty(t, message.Subject)
		return nil
	}
	err := hub.Publish(ctx, fooMessage{
		Foo: "foo",
	})
	assert.NoError(t, err)

	noopPublisher.onPublish = func(ctx context.Context, message streamhub.Message) error {
		assert.Equal(t, "bar", message.Subject)
		return nil
	}
	err = hub.Publish(ctx, fooEvent{
		Foo: "foo",
		Bar: "bar",
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

func TestHub_PublishRawMessageBatch(t *testing.T) {
	hub := streamhub.NewHub()
	hub.Publisher = nil
	ctx := context.Background()

	messageBuffer := []streamhub.Message{
		streamhub.NewMessage(streamhub.NewMessageArgs{
			SchemaVersion:        9,
			Data:                 []byte("hello foo"),
			ID:                   "123",
			Source:               "",
			Stream:               "foo-stream",
			SchemaDefinitionName: "",
			ContentType:          "",
		}), streamhub.NewMessage(streamhub.NewMessageArgs{
			SchemaVersion:        9,
			Data:                 []byte("hello bar"),
			ID:                   "abc",
			Source:               "",
			Stream:               "bar-stream",
			SchemaDefinitionName: "",
			ContentType:          "",
		}),
	}

	err := hub.PublishRawMessageBatch(ctx, messageBuffer...)
	assert.ErrorIs(t, err, streamhub.ErrMissingPublisherDriver)

	totalMessagesPushed := 0
	hub.Publisher = publisherNoopHook{
		onPublishBatch: func(_ context.Context, messages ...streamhub.Message) error {
			totalMessagesPushed = len(messages)
			return nil
		},
	}
	err = hub.PublishRawMessageBatch(ctx, messageBuffer...)
	assert.NoError(t, err)
	assert.Equal(t, len(messageBuffer), totalMessagesPushed)

	// testing noopPublisher from streamhub package to increase test coverage
	hub.Publisher = streamhub.NoopPublisher
	err = hub.PublishRawMessageBatch(ctx, messageBuffer...)
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

func TestHub_PublishByMessageKeyBatch(t *testing.T) {
	hub := streamhub.NewHub(streamhub.WithSchemaRegistry(streamhub.NoopSchemaRegistry{}))
	ctx := context.Background()
	err := hub.PublishByMessageKeyBatch(ctx, map[string]interface{}{
		"foo": fooMessage{
			Foo: "foo",
		},
	})
	assert.ErrorIs(t, err, streamhub.ErrMissingStream)

	hub.RegisterStreamByString("foo_custom", streamhub.StreamMetadata{
		Stream:               "foo-stream",
		SchemaDefinitionName: "",
		SchemaVersion:        0,
	})

	hub.IDFactory = failingFakeIDFactory
	err = hub.PublishByMessageKeyBatch(ctx, map[string]interface{}{
		"foo_custom": fooMessage{
			Foo: "custom",
		},
	})
	assert.EqualValues(t, errors.New("generic id factory error"), err)

	hub.IDFactory = streamhub.RandInt64Factory
	err = hub.PublishByMessageKeyBatch(ctx, map[string]interface{}{
		"foo_custom": fooMessage{
			Foo: "custom",
		},
	})
	assert.NoError(t, err)
}

func TestHub_Listen(t *testing.T) {
	h := streamhub.NewHub()
	err := h.Listen(fooMessage{})
	assert.ErrorIs(t, err, streamhub.ErrMissingStream)

	h.StreamRegistry.Set(fooMessage{}, streamhub.StreamMetadata{})
	err = h.Listen(fooMessage{})
	assert.NoError(t, err)
}

func TestHub_Start(t *testing.T) {
	h := streamhub.NewHub()

	h.StreamRegistry.Set(fooMessage{}, streamhub.StreamMetadata{
		Stream: "foo-stream",
	})
	err := h.Listen(fooMessage{})
	assert.NoError(t, err)

	baseCtx, cancel := context.WithTimeout(context.Background(), time.Millisecond)
	defer cancel()
	h.Start(baseCtx)

	// ListenerNode scheduler will not schedule if a ListenerDriver wasn't defined at either Hub (BaseListenerDriver)
	// level or ListenerNode level
	assert.Equal(t, 2, runtime.NumGoroutine())

	h.StreamRegistry.Set(fooMessage{}, streamhub.StreamMetadata{
		Stream: "foo-stream",
	})
	err = h.Listen(fooMessage{}, streamhub.WithDriver(listenerDriverNoopGoroutine{}))
	assert.NoError(t, err)

	baseCtx2, cancel2 := context.WithTimeout(context.Background(), time.Millisecond)
	defer cancel2()
	h.Start(baseCtx2)

	assert.Equal(t, 3, runtime.NumGoroutine())
	time.Sleep(time.Millisecond * 100)
	assert.Equal(t, 2, runtime.NumGoroutine())
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

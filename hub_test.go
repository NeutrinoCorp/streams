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

func TestHub_Write(t *testing.T) {
	hub := streamhub.NewHub(streamhub.WithSchemaRegistry(streamhub.NoopSchemaRegistry{}))
	hub.Writer = nil
	ctx := context.Background()
	err := hub.Write(ctx, fooMessage{
		Foo: "foo",
	})
	assert.ErrorIs(t, err, streamhub.ErrMissingStream)

	hub.RegisterStream(fooMessage{}, streamhub.StreamMetadata{
		Stream:               "foo-stream",
		SchemaDefinitionName: "",
		SchemaVersion:        0,
	})
	err = hub.Write(ctx, fooMessage{
		Foo: "foo",
	})
	assert.ErrorIs(t, err, streamhub.ErrMissingWriterDriver)

	hub.Writer = streamhub.NoopWriter
	err = hub.Write(ctx, fooMessage{
		Foo: "foo",
	})
	assert.NoError(t, err)
}

func TestHub_WriteBatch(t *testing.T) {
	hub := streamhub.NewHub(streamhub.WithSchemaRegistry(streamhub.NoopSchemaRegistry{}))
	hub.Writer = nil
	ctx := context.Background()
	err := hub.WriteBatch(ctx, fooMessage{
		Foo: "foo",
	})
	assert.ErrorIs(t, err, streamhub.ErrMissingStream)

	hub.RegisterStream(fooMessage{}, streamhub.StreamMetadata{
		Stream:               "foo-stream",
		SchemaDefinitionName: "",
		SchemaVersion:        0,
	})
	err = hub.WriteBatch(ctx, fooMessage{
		Foo: "foo",
	})
	assert.ErrorIs(t, err, streamhub.ErrMissingWriterDriver)

	hub.Writer = streamhub.NoopWriter
	hub.IDFactory = failingFakeIDFactory
	err = hub.WriteBatch(ctx, fooMessage{
		Foo: "foo",
	})
	assert.EqualValues(t, errors.New("generic id factory error"), err)

	hub.IDFactory = streamhub.RandInt64Factory
	err = hub.WriteBatch(ctx, fooMessage{
		Foo: "foo",
	})
	assert.NoError(t, err)
}

func TestHub_Write_Func(t *testing.T) {
	hub := streamhub.NewHub(streamhub.WithSchemaRegistry(streamhub.NoopSchemaRegistry{}))
	ctx := context.Background()
	err := hub.Write(ctx, fooMessage{
		Foo: "foo",
	})
	assert.ErrorIs(t, err, streamhub.ErrMissingStream)

	hub.RegisterStream(fooMessage{}, streamhub.StreamMetadata{
		Stream:               "foo-stream",
		SchemaDefinitionName: "",
		SchemaVersion:        0,
	})
	err = hub.Write(ctx, fooMessage{
		Foo: "foo",
	})
	assert.NoError(t, err)
}

type fooEvent struct {
	Foo string `json:"foo"`
	Bar string `json:"bar"`
}

var _ streamhub.Event = fooEvent{}

func (f fooEvent) GetSubject() string {
	return f.Bar
}

func TestHub_Write_Event(t *testing.T) {
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

	noopWriter := &writerNoopHook{}
	hub.Writer = noopWriter
	noopWriter.onWrite = func(ctx context.Context, message streamhub.Message) error {
		assert.Empty(t, message.Subject)
		return nil
	}
	err := hub.Write(ctx, fooMessage{
		Foo: "foo",
	})
	assert.NoError(t, err)

	noopWriter.onWrite = func(ctx context.Context, message streamhub.Message) error {
		assert.Equal(t, "bar", message.Subject)
		return nil
	}
	err = hub.Write(ctx, fooEvent{
		Foo: "foo",
		Bar: "bar",
	})
	assert.NoError(t, err)
}

func TestHub_Write_With_Bad_Marshaling(t *testing.T) {
	hub := streamhub.NewHub(streamhub.WithSchemaRegistry(streamhub.NoopSchemaRegistry{}),
		streamhub.WithMarshaler(failingFakeMarshaler{}))
	ctx := context.Background()

	hub.RegisterStream(fooMessage{}, streamhub.StreamMetadata{
		Stream:               "foo-stream",
		SchemaDefinitionName: "",
		SchemaVersion:        0,
	})
	err := hub.Write(ctx, fooMessage{
		Foo: "foo",
	})
	assert.Error(t, err)
}

func TestHub_Write_With_Bad_ID_Factory(t *testing.T) {
	hub := streamhub.NewHub(streamhub.WithSchemaRegistry(streamhub.NoopSchemaRegistry{}),
		streamhub.WithIDFactory(failingFakeIDFactory))
	ctx := context.Background()

	hub.RegisterStream(fooMessage{}, streamhub.StreamMetadata{
		Stream:               "foo-stream",
		SchemaDefinitionName: "",
		SchemaVersion:        0,
	})
	err := hub.Write(ctx, fooMessage{
		Foo: "foo",
	})
	assert.Error(t, err)
}

func TestHub_WriteRawMessage(t *testing.T) {
	hub := streamhub.NewHub()
	ctx := context.Background()

	err := hub.WriteRawMessage(ctx, streamhub.NewMessage(streamhub.NewMessageArgs{
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

func TestHub_WriteRawMessageBatch(t *testing.T) {
	hub := streamhub.NewHub()
	hub.Writer = nil
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

	err := hub.WriteRawMessageBatch(ctx, messageBuffer...)
	assert.ErrorIs(t, err, streamhub.ErrMissingWriterDriver)

	totalMessagesPushed := 0
	hub.Writer = writerNoopHook{
		onWriteBatch: func(_ context.Context, messages ...streamhub.Message) error {
			totalMessagesPushed = len(messages)
			return nil
		},
	}
	err = hub.WriteRawMessageBatch(ctx, messageBuffer...)
	assert.NoError(t, err)
	assert.Equal(t, len(messageBuffer), totalMessagesPushed)

	// testing noopWriter from streamhub package to increase test coverage
	hub.Writer = streamhub.NoopWriter
	err = hub.WriteRawMessageBatch(ctx, messageBuffer...)
	assert.NoError(t, err)
}

func TestHub_WriteInMemorySchemaRegistry(t *testing.T) {
	r := streamhub.InMemorySchemaRegistry{}
	hub := streamhub.NewHub(streamhub.WithSchemaRegistry(r))
	ctx := context.Background()
	hub.RegisterStream(fooMessage{}, streamhub.StreamMetadata{
		Stream:               "foo-stream",
		SchemaDefinitionName: "foo",
		SchemaVersion:        7,
	})
	err := hub.Write(ctx, fooMessage{
		Foo: "foo",
	})
	assert.ErrorIs(t, err, streamhub.ErrMissingSchemaDefinition)

	r.RegisterDefinition("foo", "sample_foo_format", 7)
	err = hub.Write(ctx, fooMessage{
		Foo: "foo",
	})
	assert.NoError(t, err)
}

func TestHub_WriteByMessageKey(t *testing.T) {
	hub := streamhub.NewHub(streamhub.WithSchemaRegistry(streamhub.NoopSchemaRegistry{}))
	ctx := context.Background()
	err := hub.WriteByMessageKey(ctx, "foo", fooMessage{
		Foo: "foo",
	})
	assert.ErrorIs(t, err, streamhub.ErrMissingStream)

	hub.RegisterStreamByString("foo_custom", streamhub.StreamMetadata{
		Stream:               "foo-stream",
		SchemaDefinitionName: "",
		SchemaVersion:        0,
	})
	err = hub.WriteByMessageKey(ctx, "foo_custom", fooMessage{
		Foo: "custom",
	})
	assert.NoError(t, err)
}

func TestHub_WriteByMessageKeyBatch(t *testing.T) {
	hub := streamhub.NewHub(streamhub.WithSchemaRegistry(streamhub.NoopSchemaRegistry{}))
	ctx := context.Background()
	err := hub.WriteByMessageKeyBatch(ctx, map[string]interface{}{
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
	err = hub.WriteByMessageKeyBatch(ctx, map[string]interface{}{
		"foo_custom": fooMessage{
			Foo: "custom",
		},
	})
	assert.EqualValues(t, errors.New("generic id factory error"), err)

	hub.IDFactory = streamhub.RandInt64Factory
	err = hub.WriteByMessageKeyBatch(ctx, map[string]interface{}{
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

func BenchmarkHub_Write(b *testing.B) {
	hub := streamhub.NewHub()
	hub.RegisterStream(fooMessage{}, streamhub.StreamMetadata{
		Stream: "foo-stream",
	})
	msg := fooMessage{
		Foo: "1",
	}

	for i := 0; i < b.N; i++ {
		b.ReportAllocs()
		err := hub.Write(context.Background(), msg)
		if err != nil {
			panic(err)
		}
	}
}

func BenchmarkHub_Write_With_Schema_Registry(b *testing.B) {
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
		err := hub.Write(context.Background(), msg)
		if err != nil {
			panic(err)
		}
	}
}

func BenchmarkHub_WriteByMessageKey(b *testing.B) {
	hub := streamhub.NewHub()
	hub.RegisterStreamByString("fooMessage", streamhub.StreamMetadata{
		Stream: "foo-stream",
	})
	msg := fooMessage{
		Foo: "1",
	}

	for i := 0; i < b.N; i++ {
		b.ReportAllocs()
		err := hub.WriteByMessageKey(context.Background(), "fooMessage", msg)
		if err != nil {
			panic(err)
		}
	}
}

func BenchmarkHub_WriteRawMessage(b *testing.B) {
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
		err := hub.WriteRawMessage(context.Background(), streamhub.NewMessage(args))
		if err != nil {
			panic(err)
		}
	}
}

func BenchmarkHub_WritePreBuildRawMessage(b *testing.B) {
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
		err := hub.WriteRawMessage(context.Background(), msg)
		if err != nil {
			panic(err)
		}
	}
}

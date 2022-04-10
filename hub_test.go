package streams_test

import (
	"context"
	"errors"
	"runtime"
	"strconv"
	"testing"
	"time"

	"github.com/neutrinocorp/streams"
	"github.com/stretchr/testify/assert"
)

func TestHub_Write(t *testing.T) {
	hub := streams.NewHub(streams.WithSchemaRegistry(streams.NoopSchemaRegistry{}))
	hub.Writer = nil
	ctx := context.Background()
	err := hub.Write(ctx, fooMessage{
		Foo: "foo",
	})
	assert.ErrorIs(t, err, streams.ErrMissingStream)

	hub.RegisterStream(fooMessage{}, streams.StreamMetadata{
		Stream:               "foo-stream",
		SchemaDefinitionName: "",
		SchemaVersion:        0,
	})
	err = hub.Write(ctx, fooMessage{
		Foo: "foo",
	})
	assert.ErrorIs(t, err, streams.ErrMissingWriterDriver)

	hub.Writer = streams.NoopWriter
	err = hub.Write(ctx, fooMessage{
		Foo: "foo",
	})
	assert.NoError(t, err)
}

func TestHub_WriteBatch(t *testing.T) {
	hub := streams.NewHub(streams.WithSchemaRegistry(streams.NoopSchemaRegistry{}))
	hub.Writer = nil
	ctx := context.Background()
	_, err := hub.WriteBatch(ctx, fooMessage{
		Foo: "foo",
	})
	assert.ErrorIs(t, err, streams.ErrMissingStream)

	hub.RegisterStream(fooMessage{}, streams.StreamMetadata{
		Stream:               "foo-stream",
		SchemaDefinitionName: "",
		SchemaVersion:        0,
	})
	_, err = hub.WriteBatch(ctx, fooMessage{
		Foo: "foo",
	})
	assert.ErrorIs(t, err, streams.ErrMissingWriterDriver)

	hub.Writer = streams.NoopWriter
	hub.IDFactory = failingFakeIDFactory
	_, err = hub.WriteBatch(ctx, fooMessage{
		Foo: "foo",
	})
	assert.EqualValues(t, errors.New("generic id factory error"), err)

	hub.IDFactory = streams.RandInt64Factory
	_, err = hub.WriteBatch(ctx, fooMessage{
		Foo: "foo",
	})
	assert.NoError(t, err)
}

func TestHub_Write_Func(t *testing.T) {
	hub := streams.NewHub(streams.WithSchemaRegistry(streams.NoopSchemaRegistry{}))
	ctx := context.Background()
	err := hub.Write(ctx, fooMessage{
		Foo: "foo",
	})
	assert.ErrorIs(t, err, streams.ErrMissingStream)

	hub.RegisterStream(fooMessage{}, streams.StreamMetadata{
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

var _ streams.Event = fooEvent{}

func (f fooEvent) GetSubject() string {
	return f.Bar
}

func TestHub_Write_Event(t *testing.T) {
	hub := streams.NewHub(streams.WithSchemaRegistry(streams.NoopSchemaRegistry{}))
	ctx := context.Background()
	hub.RegisterStream(fooMessage{}, streams.StreamMetadata{
		Stream:               "foo-stream",
		SchemaDefinitionName: "",
		SchemaVersion:        0,
	})
	hub.RegisterStream(fooEvent{}, streams.StreamMetadata{
		Stream:               "foo-stream",
		SchemaDefinitionName: "",
		SchemaVersion:        0,
	})

	noopWriter := &writerNoopHook{}
	hub.Writer = noopWriter
	noopWriter.onWrite = func(ctx context.Context, message streams.Message) error {
		assert.Empty(t, message.Subject)
		return nil
	}
	err := hub.Write(ctx, fooMessage{
		Foo: "foo",
	})
	assert.NoError(t, err)

	noopWriter.onWrite = func(ctx context.Context, message streams.Message) error {
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
	hub := streams.NewHub(streams.WithSchemaRegistry(streams.NoopSchemaRegistry{}),
		streams.WithMarshaler(failingFakeMarshaler{}))
	ctx := context.Background()

	hub.RegisterStream(fooMessage{}, streams.StreamMetadata{
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
	hub := streams.NewHub(streams.WithSchemaRegistry(streams.NoopSchemaRegistry{}),
		streams.WithIDFactory(failingFakeIDFactory))
	ctx := context.Background()

	hub.RegisterStream(fooMessage{}, streams.StreamMetadata{
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
	hub := streams.NewHub()
	ctx := context.Background()

	err := hub.WriteRawMessage(ctx, streams.NewMessage(streams.NewMessageArgs{
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
	hub := streams.NewHub()
	hub.Writer = nil
	ctx := context.Background()

	messageBuffer := []streams.Message{
		streams.NewMessage(streams.NewMessageArgs{
			SchemaVersion:        9,
			Data:                 []byte("hello foo"),
			ID:                   "123",
			Source:               "",
			Stream:               "foo-stream",
			SchemaDefinitionName: "",
			ContentType:          "",
		}), streams.NewMessage(streams.NewMessageArgs{
			SchemaVersion:        9,
			Data:                 []byte("hello bar"),
			ID:                   "abc",
			Source:               "",
			Stream:               "bar-stream",
			SchemaDefinitionName: "",
			ContentType:          "",
		}),
	}

	out, err := hub.WriteRawMessageBatch(ctx, messageBuffer...)
	assert.ErrorIs(t, err, streams.ErrMissingWriterDriver)
	assert.Equal(t, uint32(0), out)

	totalMessagesPushed := 0
	hub.Writer = writerNoopHook{
		onWriteBatch: func(_ context.Context, messages ...streams.Message) (uint32, error) {
			totalMessagesPushed = len(messages)
			return uint32(totalMessagesPushed), nil
		},
	}
	out, err = hub.WriteRawMessageBatch(ctx, messageBuffer...)
	assert.NoError(t, err)
	assert.Equal(t, len(messageBuffer), totalMessagesPushed)
	assert.Equal(t, uint32(len(messageBuffer)), out)

	// testing noopWriter from streams package to increase test coverage
	hub.Writer = streams.NoopWriter
	_, err = hub.WriteRawMessageBatch(ctx, messageBuffer...)
	assert.NoError(t, err)
}

func TestHub_WriteInMemorySchemaRegistry(t *testing.T) {
	r := streams.InMemorySchemaRegistry{}
	hub := streams.NewHub(streams.WithSchemaRegistry(r))
	ctx := context.Background()
	hub.RegisterStream(fooMessage{}, streams.StreamMetadata{
		Stream:               "foo-stream",
		SchemaDefinitionName: "foo",
		SchemaVersion:        7,
	})
	err := hub.Write(ctx, fooMessage{
		Foo: "foo",
	})
	assert.ErrorIs(t, err, streams.ErrMissingSchemaDefinition)

	r.RegisterDefinition("foo", "sample_foo_format", 7)
	err = hub.Write(ctx, fooMessage{
		Foo: "foo",
	})
	assert.NoError(t, err)
}

func TestHub_WriteByMessageKey(t *testing.T) {
	hub := streams.NewHub(streams.WithSchemaRegistry(streams.NoopSchemaRegistry{}))
	ctx := context.Background()
	err := hub.WriteByMessageKey(ctx, "foo", fooMessage{
		Foo: "foo",
	})
	assert.ErrorIs(t, err, streams.ErrMissingStream)

	hub.RegisterStreamByString("foo_custom", streams.StreamMetadata{
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
	hub := streams.NewHub(streams.WithSchemaRegistry(streams.NoopSchemaRegistry{}))
	ctx := context.Background()
	_, err := hub.WriteByMessageKeyBatch(ctx, map[string]interface{}{
		"foo": fooMessage{
			Foo: "foo",
		},
	})
	assert.ErrorIs(t, err, streams.ErrMissingStream)

	hub.RegisterStreamByString("foo_custom", streams.StreamMetadata{
		Stream:               "foo-stream",
		SchemaDefinitionName: "",
		SchemaVersion:        0,
	})

	hub.IDFactory = failingFakeIDFactory
	_, err = hub.WriteByMessageKeyBatch(ctx, map[string]interface{}{
		"foo_custom": fooMessage{
			Foo: "custom",
		},
	})
	assert.EqualValues(t, errors.New("generic id factory error"), err)

	hub.IDFactory = streams.RandInt64Factory
	_, err = hub.WriteByMessageKeyBatch(ctx, map[string]interface{}{
		"foo_custom": fooMessage{
			Foo: "custom",
		},
	})
	assert.NoError(t, err)
}

func TestHub_Read(t *testing.T) {
	h := streams.NewHub()
	err := h.Read(fooMessage{})
	assert.ErrorIs(t, err, streams.ErrMissingStream)

	h.StreamRegistry.Set(fooMessage{}, streams.StreamMetadata{})
	err = h.Read(fooMessage{})
	assert.NoError(t, err)
}

func TestHub_Start(t *testing.T) {
	h := streams.NewHub()

	h.StreamRegistry.Set(fooMessage{}, streams.StreamMetadata{
		Stream: "foo-stream",
	})
	err := h.Read(fooMessage{})
	assert.NoError(t, err)

	baseCtx, cancel := context.WithTimeout(context.Background(), time.Millisecond)
	defer cancel()
	h.Start(baseCtx)

	// ReaderNode scheduler will not schedule if a ReaderDriver wasn't defined at either Hub (BaseReaderDriver)
	// level or ReaderNode level
	assert.Equal(t, 2, runtime.NumGoroutine())

	h.StreamRegistry.Set(fooMessage{}, streams.StreamMetadata{
		Stream: "foo-stream",
	})
	err = h.Read(fooMessage{}, streams.WithDriver(listenerDriverNoopGoroutine{}))
	assert.NoError(t, err)

	baseCtx2, cancel2 := context.WithTimeout(context.Background(), time.Millisecond)
	defer cancel2()
	h.Start(baseCtx2)

	assert.Equal(t, 3, runtime.NumGoroutine())
	time.Sleep(time.Millisecond * 100)
	assert.Equal(t, 2, runtime.NumGoroutine())
}

func BenchmarkHub_RegisterStream(b *testing.B) {
	hub := streams.NewHub()
	for i := 0; i < b.N; i++ {
		b.ReportAllocs()
		hub.RegisterStream(fooMessage{}, streams.StreamMetadata{
			Stream:               "foo-stream",
			SchemaDefinitionName: "foo_stream",
			SchemaVersion:        8,
		})
	}
}

func BenchmarkHub_RegisterStreamByString(b *testing.B) {
	hub := streams.NewHub()
	for i := 0; i < b.N; i++ {
		b.ReportAllocs()
		hub.RegisterStreamByString("fooMessage"+strconv.Itoa(i), streams.StreamMetadata{
			Stream:               "foo-stream",
			SchemaDefinitionName: "foo_stream",
			SchemaVersion:        8,
		})
	}
}

func BenchmarkHub_Write(b *testing.B) {
	hub := streams.NewHub()
	hub.RegisterStream(fooMessage{}, streams.StreamMetadata{
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
	r := streams.InMemorySchemaRegistry{}
	r.RegisterDefinition("foo-stream", "elver", 0)
	hub := streams.NewHub(streams.WithSchemaRegistry(r))
	hub.RegisterStream(fooMessage{}, streams.StreamMetadata{
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
	hub := streams.NewHub()
	hub.RegisterStreamByString("fooMessage", streams.StreamMetadata{
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
	hub := streams.NewHub()
	args := streams.NewMessageArgs{
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
		err := hub.WriteRawMessage(context.Background(), streams.NewMessage(args))
		if err != nil {
			panic(err)
		}
	}
}

func BenchmarkHub_WritePreBuildRawMessage(b *testing.B) {
	hub := streams.NewHub()
	msg := streams.NewMessage(streams.NewMessageArgs{ // 2 allocs
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

package streamhub

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/modern-go/reflect2"

	jsoniter "github.com/json-iterator/go"
	"github.com/stretchr/testify/assert"
)

func TestReaderNodeHandlerBehaviour_Retry(t *testing.T) {
	var h ReaderHandleFunc = func(ctx context.Context, message Message) error {
		return errors.New("generic error")
	}
	baseOpts := &ReaderNode{
		RetryInitialInterval: time.Millisecond,
		RetryMaxInterval:     time.Millisecond * 10,
		RetryTimeout:         time.Millisecond * 2,
	}
	h = retryReaderBehaviour(baseOpts, nil, h)
	defer func(startTime time.Time) {
		retryTime := time.Since(startTime)
		assert.GreaterOrEqual(t, time.Millisecond*3, retryTime)
	}(time.Now())
	err := h(context.Background(), Message{})
	assert.Error(t, err)
}

type fooMessage struct {
	Hello string `json:"hello"`
}

func TestReaderNodeHandlerBehaviour_Unmarshal(t *testing.T) {
	var h ReaderHandleFunc = func(ctx context.Context, message Message) error {
		// streamhub now passes decoded data as pointer
		// This is caused by the usage of the reflect2 package and the new protobuf implementation.
		//
		// Protobuf messages are complex structures with inner data components such as sync.Mutex which require a
		// pointer struct to avoid data copies
		dataValid, ok := message.DecodedData.(*fooMessage)
		assert.True(t, ok)
		_, ok = message.DecodedData.(fooMessage)
		assert.False(t, ok)
		assert.Equal(t, "foo", dataValid.Hello)
		return nil
	}
	hub := NewHub(WithSchemaRegistry(InMemorySchemaRegistry{
		"foo": "foobarbaz",
	}))
	h = unmarshalReaderBehaviour(&ReaderNode{}, hub, h)

	baseMsg := Message{
		Stream: "foo-stream",
	}

	err := h(context.Background(), baseMsg)
	assert.ErrorIs(t, err, ErrMissingStream)

	hub.StreamRegistry.SetByString("foo", StreamMetadata{Stream: "foo-stream", SchemaDefinitionName: "bar"})
	err = h(context.Background(), baseMsg)
	assert.ErrorIs(t, err, ErrMissingSchemaDefinition)

	hub.StreamRegistry.SetByString("foo", StreamMetadata{
		Stream:               "foo-stream",
		SchemaDefinitionName: "foo",
		GoType:               reflect2.TypeOf(fooMessage{}),
	})
	fooJSON, _ := jsoniter.Marshal(fooMessage{
		Hello: "foo",
	})
	assert.NotNil(t, fooJSON)

	hub.Marshaler = FailingMarshalerNoop{}
	err = h(context.Background(), Message{
		Stream: "foo-stream",
		Data:   fooJSON,
	})
	assert.Equal(t, "failing unmarshal", err.Error())

	hub.Marshaler = JSONMarshaler{}
	err = h(context.Background(), Message{
		Stream: "foo-stream",
		Data:   fooJSON,
	})
	assert.NoError(t, err)
}

func BenchmarkReaderNodeHandlerBehaviour_Unmarshal(b *testing.B) {
	var h ReaderHandleFunc = func(ctx context.Context, message Message) error {
		return nil
	}
	hub := NewHub(WithSchemaRegistry(InMemorySchemaRegistry{
		"foo": "foobarbaz",
	}))
	h = unmarshalReaderBehaviour(&ReaderNode{}, hub, h)
	hub.StreamRegistry.SetByString("foo", StreamMetadata{
		Stream:               "foo-stream",
		SchemaDefinitionName: "foo",
		GoType:               reflect2.TypeOf(fooMessage{}),
	})

	baseCtx := context.Background()
	fooJSON, _ := jsoniter.Marshal(fooMessage{
		Hello: "foo",
	})
	baseMsg := Message{
		Stream: "foo-stream",
		Data:   fooJSON,
	}
	for i := 0; i < b.N; i++ {
		b.ReportAllocs()
		_ = h(baseCtx, baseMsg)
	}
}

func TestReaderNodeHandlerBehaviour_GroupInjector(t *testing.T) {
	var h ReaderHandleFunc = func(ctx context.Context, message Message) error {
		assert.Equal(t, "foo", message.GroupName)
		return nil
	}
	baseOpts := &ReaderNode{
		Group: "foo",
	}
	h = injectGroupReaderBehaviour(baseOpts, nil, h)
	err := h(context.Background(), Message{})
	assert.NoError(t, err)
}

func TestReaderNodeHandlerBehaviour_TxInjector(t *testing.T) {
	const (
		correlationId = "1"
		id            = "abc"
	)

	var h ReaderHandleFunc = func(ctx context.Context, message Message) error {
		scopedCorr, ok := ctx.Value(ContextCorrelationID).(MessageContextKey)
		assert.True(t, ok)
		assert.Equal(t, correlationId, string(scopedCorr))
		scopedCau, ok := ctx.Value(ContextCausationID).(MessageContextKey)
		assert.True(t, ok)
		assert.Equal(t, id, string(scopedCau))
		// scoped ctx differs from the message data as it prepares publishers to push list's previous HEAD as causation
		// id
		assert.Equal(t, correlationId, message.CausationID)
		return nil
	}
	baseOpts := &ReaderNode{}
	h = injectTxIDsReaderBehaviour(baseOpts, nil, h)
	err := h(context.Background(), Message{
		ID:            id,
		CorrelationID: correlationId,
		CausationID:   correlationId,
	})
	assert.NoError(t, err)
}

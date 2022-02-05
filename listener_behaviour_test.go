package streamhub

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	jsoniter "github.com/json-iterator/go"
	"github.com/stretchr/testify/assert"
)

func TestListenerNodeHandlerBehaviour_Retry(t *testing.T) {
	var h ListenerFunc = func(ctx context.Context, message Message) error {
		return errors.New("generic error")
	}
	baseOpts := &ListenerNode{
		RetryInitialInterval: time.Millisecond,
		RetryMaxInterval:     time.Millisecond * 10,
		RetryTimeout:         time.Millisecond * 2,
	}
	h = retryListenerBehaviour(baseOpts, nil, h)
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

func TestListenerNodeHandlerBehaviour_Unmarshal(t *testing.T) {
	var h ListenerFunc = func(ctx context.Context, message Message) error {
		_, ok := message.DecodedData.(*fooMessage)
		assert.False(t, ok)
		dataValid, ok := message.DecodedData.(fooMessage)
		assert.True(t, ok)
		assert.Equal(t, "foo", dataValid.Hello)
		return nil
	}
	hub := NewHub(WithSchemaRegistry(InMemorySchemaRegistry{
		"foo": "foobarbaz",
	}))
	h = unmarshalListenerBehaviour(&ListenerNode{}, hub, h)

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
		GoType:               reflect.TypeOf(fooMessage{}),
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

func BenchmarkListenerNodeHandlerBehaviour_Unmarshal(b *testing.B) {
	var h ListenerFunc = func(ctx context.Context, message Message) error {
		return nil
	}
	hub := NewHub(WithSchemaRegistry(InMemorySchemaRegistry{
		"foo": "foobarbaz",
	}))
	h = unmarshalListenerBehaviour(&ListenerNode{}, hub, h)
	hub.StreamRegistry.SetByString("foo", StreamMetadata{
		Stream:               "foo-stream",
		SchemaDefinitionName: "foo",
		GoType:               reflect.TypeOf(fooMessage{}),
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

func TestListenerNodeHandlerBehaviour_GroupInjector(t *testing.T) {
	var h ListenerFunc = func(ctx context.Context, message Message) error {
		assert.Equal(t, "foo", message.GroupName)
		return nil
	}
	baseOpts := &ListenerNode{
		Group: "foo",
	}
	h = injectGroupListenerBehaviour(baseOpts, nil, h)
	err := h(context.Background(), Message{})
	assert.NoError(t, err)
}

func TestListenerNodeHandlerBehaviour_TxInjector(t *testing.T) {
	const (
		correlationId = "1"
		id            = "abc"
	)

	var h ListenerFunc = func(ctx context.Context, message Message) error {
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
	baseOpts := &ListenerNode{}
	h = injectTxIDsListenerBehaviour(baseOpts, nil, h)
	err := h(context.Background(), Message{
		ID:            id,
		CorrelationID: correlationId,
		CausationID:   correlationId,
	})
	assert.NoError(t, err)
}

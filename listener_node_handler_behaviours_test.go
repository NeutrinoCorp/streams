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
	var h ListenerNodeHandler = func(ctx context.Context, message Message) error {
		return errors.New("generic error")
	}
	baseOpts := listenerNodeOptions{
		retryInitialInterval: time.Millisecond,
		retryMaxInterval:     time.Millisecond * 10,
		retryTimeout:         time.Millisecond * 2,
	}
	h = retryListenerNodeBehaviour(baseOpts, h)
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
	var h ListenerNodeHandler = func(ctx context.Context, message Message) error {
		return nil
	}
	hub := NewHub(WithSchemaRegistry(InMemorySchemaRegistry{
		"foo": "foobarbaz",
	}))
	h = unmarshalListenerNodeBehaviour(hub, h)

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

func TestListenerNodeHandlerBehaviour_GroupInjector(t *testing.T) {
	var h ListenerNodeHandler = func(ctx context.Context, message Message) error {
		assert.Equal(t, "foo", message.GroupName)
		return nil
	}
	baseOpts := listenerNodeOptions{
		group: "foo",
	}
	h = injectGroupListenerNodeBehaviour(baseOpts, h)
	err := h(context.Background(), Message{})
	assert.NoError(t, err)
}

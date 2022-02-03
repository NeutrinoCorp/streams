package streamhub_test

import (
	"context"
	"testing"

	"github.com/neutrinocorp/streamhub"
	"github.com/stretchr/testify/assert"
)

func TestWithInstanceName(t *testing.T) {
	hub := streamhub.NewHub()
	assert.Equal(t, "com.streamhub", hub.InstanceName)

	hub = streamhub.NewHub(
		streamhub.WithInstanceName("org.neutrino"))
	assert.Equal(t, "org.neutrino", hub.InstanceName)
}

func TestWithPublisherFunc(t *testing.T) {
	hub := streamhub.NewHub()
	assert.IsType(t, streamhub.NoopPublisherFunc, hub.PublisherFunc)

	hub = streamhub.NewHub(
		streamhub.WithPublisherFunc(streamhub.NoopPublisherFunc))
	assert.IsType(t, streamhub.NoopPublisherFunc, hub.PublisherFunc)
}

func TestWithPublisher(t *testing.T) {
	hub := streamhub.NewHub()
	assert.Nil(t, hub.Publisher)

	hub = streamhub.NewHub(
		streamhub.WithPublisher(streamhub.NoopPublisher))
	assert.IsType(t, streamhub.NoopPublisher, hub.Publisher)
}

func TestWithMarshaler(t *testing.T) {
	hub := streamhub.NewHub()
	assert.IsType(t, streamhub.JSONMarshaler{}, hub.Marshaler)

	hub = streamhub.NewHub(
		streamhub.WithMarshaler(streamhub.JSONMarshaler{}))
	assert.IsType(t, streamhub.JSONMarshaler{}, hub.Marshaler)
}

type listenerDriverNoop struct{}

var _ streamhub.ListenerDriver = listenerDriverNoop{}

// ExecuteTask the no-operation implementation of ListenerDriver
func (l listenerDriverNoop) ExecuteTask(_ context.Context, _ streamhub.ListenerTask) error {
	return nil
}

func TestWithBaseDriver(t *testing.T) {
	hub := streamhub.NewHub()
	assert.Empty(t, hub.BaseListenerDriver)

	hub = streamhub.NewHub(
		streamhub.WithBaseDriver(listenerDriverNoop{}))
	assert.IsType(t, listenerDriverNoop{}, hub.BaseListenerDriver)
}

func TestWithIDFactory(t *testing.T) {
	hub := streamhub.NewHub()
	assert.IsType(t, streamhub.UuidIdFactory, hub.IDFactory)

	hub = streamhub.NewHub(
		streamhub.WithIDFactory(streamhub.UuidIdFactory))
	assert.IsType(t, streamhub.UuidIdFactory, hub.IDFactory)
}

func TestWithSchemaRegistry(t *testing.T) {
	hub := streamhub.NewHub()
	assert.Nil(t, hub.SchemaRegistry)

	hub = streamhub.NewHub(
		streamhub.WithSchemaRegistry(streamhub.NoopSchemaRegistry{}))
	assert.IsType(t, streamhub.NoopSchemaRegistry{}, hub.SchemaRegistry)
}

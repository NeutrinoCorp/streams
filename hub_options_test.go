package streamhub_test

import (
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

func TestWithPublisher(t *testing.T) {
	hub := streamhub.NewHub()
	assert.IsType(t, streamhub.NoopPublisher, hub.Publisher)

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

func TestWithListenerDriver(t *testing.T) {
	hub := streamhub.NewHub()
	assert.Empty(t, hub.ListenerDriver)

	hub = streamhub.NewHub(
		streamhub.WithListenerDriver(listenerDriverNoop{}))
	assert.IsType(t, listenerDriverNoop{}, hub.ListenerDriver)
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

func TestWithListenerBehaviours(t *testing.T) {
	hub := streamhub.NewHub()
	assert.NotNil(t, hub.ListenerBehaviours)

	totalDefaultBehaviours := len(hub.ListenerBehaviours)
	hub = streamhub.NewHub(
		streamhub.WithListenerBehaviours(func(node *streamhub.ListenerNode, hub *streamhub.Hub, next streamhub.ListenerFunc) streamhub.ListenerFunc {
			return next
		}))
	// verify if no default behaviour was removed
	assert.Equal(t, totalDefaultBehaviours+1, len(hub.ListenerBehaviours))
}

func TestWithListenerBaseOptions(t *testing.T) {
	hub := streamhub.NewHub()
	assert.NotNil(t, hub.ListenerBaseOptions)

	totalDefaultOpts := len(hub.ListenerBaseOptions)
	hub = streamhub.NewHub(
		streamhub.WithListenerBaseOptions(streamhub.WithGroup("foo")))
	// verify if no default behaviour was removed
	assert.Equal(t, totalDefaultOpts+1, len(hub.ListenerBaseOptions))
}

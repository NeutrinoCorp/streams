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

func TestWithWriter(t *testing.T) {
	hub := streamhub.NewHub()
	assert.IsType(t, streamhub.NoopWriter, hub.Writer)

	hub = streamhub.NewHub(
		streamhub.WithWriter(streamhub.NoopWriter))
	assert.IsType(t, streamhub.NoopWriter, hub.Writer)
}

func TestWithMarshaler(t *testing.T) {
	hub := streamhub.NewHub()
	assert.IsType(t, streamhub.JSONMarshaler{}, hub.Marshaler)

	hub = streamhub.NewHub(
		streamhub.WithMarshaler(streamhub.JSONMarshaler{}))
	assert.IsType(t, streamhub.JSONMarshaler{}, hub.Marshaler)
}

func TestWithReader(t *testing.T) {
	hub := streamhub.NewHub()
	assert.Empty(t, hub.Reader)

	hub = streamhub.NewHub(
		streamhub.WithReader(listenerDriverNoop{}))
	assert.IsType(t, listenerDriverNoop{}, hub.Reader)
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

func TestWithReaderBehaviours(t *testing.T) {
	hub := streamhub.NewHub()
	assert.NotNil(t, hub.ReaderBehaviours)

	totalDefaultBehaviours := len(hub.ReaderBehaviours)
	hub = streamhub.NewHub(
		streamhub.WithReaderBehaviours(func(node *streamhub.ReaderNode, hub *streamhub.Hub,
			next streamhub.ReaderHandleFunc) streamhub.ReaderHandleFunc {
			return next
		}))
	// verify if no default behaviour was removed
	assert.Equal(t, totalDefaultBehaviours+1, len(hub.ReaderBehaviours))
}

func TestWithReaderBaseOptions(t *testing.T) {
	hub := streamhub.NewHub()
	assert.NotNil(t, hub.ReaderBaseOptions)

	totalDefaultOpts := len(hub.ReaderBaseOptions)
	hub = streamhub.NewHub(
		streamhub.WithReaderBaseOptions(streamhub.WithGroup("foo")))
	// verify if no default behaviour was removed
	assert.Equal(t, totalDefaultOpts+1, len(hub.ReaderBaseOptions))
}

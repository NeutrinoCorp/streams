package streams_test

import (
	"testing"

	"github.com/neutrinocorp/streams"
	"github.com/stretchr/testify/assert"
)

func TestWithInstanceName(t *testing.T) {
	hub := streams.NewHub()
	assert.Equal(t, "com.streams", hub.InstanceName)

	hub = streams.NewHub(
		streams.WithInstanceName("org.neutrino"))
	assert.Equal(t, "org.neutrino", hub.InstanceName)
}

func TestWithWriter(t *testing.T) {
	hub := streams.NewHub()
	assert.IsType(t, streams.NoopWriter, hub.Writer)

	hub = streams.NewHub(
		streams.WithWriter(streams.NoopWriter))
	assert.IsType(t, streams.NoopWriter, hub.Writer)
}

func TestWithMarshaler(t *testing.T) {
	hub := streams.NewHub()
	assert.IsType(t, streams.JSONMarshaler{}, hub.Marshaler)

	hub = streams.NewHub(
		streams.WithMarshaler(streams.JSONMarshaler{}))
	assert.IsType(t, streams.JSONMarshaler{}, hub.Marshaler)
}

func TestWithReader(t *testing.T) {
	hub := streams.NewHub()
	assert.Empty(t, hub.Reader)

	hub = streams.NewHub(
		streams.WithReader(listenerDriverNoop{}))
	assert.IsType(t, listenerDriverNoop{}, hub.Reader)
}

func TestWithIDFactory(t *testing.T) {
	hub := streams.NewHub()
	assert.IsType(t, streams.UuidIdFactory, hub.IDFactory)

	hub = streams.NewHub(
		streams.WithIDFactory(streams.UuidIdFactory))
	assert.IsType(t, streams.UuidIdFactory, hub.IDFactory)
}

func TestWithSchemaRegistry(t *testing.T) {
	hub := streams.NewHub()
	assert.Nil(t, hub.SchemaRegistry)

	hub = streams.NewHub(
		streams.WithSchemaRegistry(streams.NoopSchemaRegistry{}))
	assert.IsType(t, streams.NoopSchemaRegistry{}, hub.SchemaRegistry)
}

func TestWithReaderBehaviours(t *testing.T) {
	hub := streams.NewHub()
	assert.NotNil(t, hub.ReaderBehaviours)

	totalDefaultBehaviours := len(hub.ReaderBehaviours)
	hub = streams.NewHub(
		streams.WithReaderBehaviours(func(node *streams.ReaderNode, hub *streams.Hub,
			next streams.ReaderHandleFunc) streams.ReaderHandleFunc {
			return next
		}))
	// verify if no default behaviour was removed
	assert.Equal(t, totalDefaultBehaviours+1, len(hub.ReaderBehaviours))
}

func TestWithReaderBaseOptions(t *testing.T) {
	hub := streams.NewHub()
	assert.NotNil(t, hub.ReaderBaseOptions)

	totalDefaultOpts := len(hub.ReaderBaseOptions)
	hub = streams.NewHub(
		streams.WithReaderBaseOptions(streams.WithGroup("foo")))
	// verify if no default behaviour was removed
	assert.Equal(t, totalDefaultOpts+1, len(hub.ReaderBaseOptions))
}

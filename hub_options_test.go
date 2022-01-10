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

func TestWithPublisherFunc(t *testing.T) {
	hub := streamhub.NewHub()
	assert.IsType(t, streamhub.NoopPublisher, hub.PublisherFunc)

	hub = streamhub.NewHub(
		streamhub.WithPublisherFunc(streamhub.NoopPublisher))
	assert.IsType(t, streamhub.NoopPublisher, hub.PublisherFunc)
}

func TestWithMarshaler(t *testing.T) {
	hub := streamhub.NewHub()
	assert.IsType(t, streamhub.JSONMarshaler{}, hub.Marshaler)

	hub = streamhub.NewHub(
		streamhub.WithMarshaler(streamhub.JSONMarshaler{}))
	assert.IsType(t, streamhub.JSONMarshaler{}, hub.Marshaler)
}

func TestWithIDFactory(t *testing.T) {
	hub := streamhub.NewHub()
	assert.IsType(t, streamhub.UuidIdFactory, hub.IDFactory)

	hub = streamhub.NewHub(
		streamhub.WithIDFactory(streamhub.UuidIdFactory))
	assert.IsType(t, streamhub.UuidIdFactory, hub.IDFactory)
}

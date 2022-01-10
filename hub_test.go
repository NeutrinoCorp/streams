package streamhub_test

import (
	"context"
	"testing"

	"github.com/neutrinocorp/streamhub"
	"github.com/stretchr/testify/assert"
)

func TestHub_Publish(t *testing.T) {
	hub := streamhub.NewHub(streamhub.WithSchemaRegistry(streamhub.NoopSchemaRegistry{}))
	ctx := context.Background()
	err := hub.Publish(ctx, fooMessage{
		Foo: "foo",
	})
	assert.ErrorIs(t, err, streamhub.ErrMissingStream)

	hub.RegisterStream(fooMessage{}, streamhub.StreamMetadata{
		Stream:           "foo-stream",
		SchemaDefinition: "",
		SchemaVersion:    0,
	})
	err = hub.Publish(ctx, fooMessage{
		Foo: "foo",
	})
	assert.NoError(t, err)
}

func TestHub_PublishInMemorySchemaRegistry(t *testing.T) {
	r := streamhub.InMemorySchemaRegistry{}
	hub := streamhub.NewHub(streamhub.WithSchemaRegistry(r))
	ctx := context.Background()
	hub.RegisterStream(fooMessage{}, streamhub.StreamMetadata{
		Stream:           "foo-stream",
		SchemaDefinition: "foo",
		SchemaVersion:    7,
	})
	err := hub.Publish(ctx, fooMessage{
		Foo: "foo",
	})
	assert.ErrorIs(t, err, streamhub.ErrMissingSchemaDefinition)

	r.RegisterDefinition("foo", "sample_foo_format", 7)
	err = hub.Publish(ctx, fooMessage{
		Foo: "foo",
	})
	assert.NoError(t, err)
}

func TestHub_PublishByMessageKey(t *testing.T) {
	hub := streamhub.NewHub(streamhub.WithSchemaRegistry(streamhub.NoopSchemaRegistry{}))
	ctx := context.Background()
	err := hub.PublishByMessageKey(ctx, "foo", fooMessage{
		Foo: "foo",
	})
	assert.ErrorIs(t, err, streamhub.ErrMissingStream)

	hub.RegisterStreamByString("foo_custom", streamhub.StreamMetadata{
		Stream:           "foo-stream",
		SchemaDefinition: "",
		SchemaVersion:    0,
	})
	err = hub.PublishByMessageKey(ctx, "foo_custom", fooMessage{
		Foo: "custom",
	})
	assert.NoError(t, err)
}

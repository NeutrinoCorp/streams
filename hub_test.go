package streamhub_test

import (
	"context"
	"testing"

	"github.com/neutrinocorp/streamhub"
	"github.com/stretchr/testify/assert"
)

func TestHub_Publish(t *testing.T) {
	hub := streamhub.NewHub()
	ctx := context.Background()
	err := hub.Publish(ctx, fooMessage{
		Foo: "foo",
	})
	assert.Error(t, err)
	hub.RegisterStream(fooMessage{}, streamhub.StreamMetadata{
		Stream:           "foo-stream",
		SchemaDefinition: "",
		SchemaVersion:    0,
	})
	err = hub.Publish(ctx, fooMessage{
		Foo: "foo",
	})
	assert.NoError(t, err)
	hub.RegisterStreamByString("foo_custom", streamhub.StreamMetadata{
		Stream:           "foo-stream",
		SchemaDefinition: "",
		SchemaVersion:    0,
	})
	err = hub.Publish(ctx, fooMessage{
		Foo: "custom",
	})
	assert.NoError(t, err)
}

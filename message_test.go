package streamhub_test

import (
	"testing"

	"github.com/neutrinocorp/streamhub"
	"github.com/stretchr/testify/assert"
)

var newMessageSuite = []struct {
	In  streamhub.NewMessageArgs
	Exp streamhub.Message
}{
	{
		In:  streamhub.NewMessageArgs{},
		Exp: streamhub.Message{},
	},
	{
		In: streamhub.NewMessageArgs{
			Data:        []byte("foo"),
			ID:          "",
			Source:      "",
			Metadata:    streamhub.StreamMetadata{},
			ContentType: "",
		},
		Exp: streamhub.Message{
			Data: []byte("foo"),
		},
	},
	{
		In: streamhub.NewMessageArgs{
			Data:        []byte("foo"),
			ID:          "123",
			Source:      "com.streamhub",
			Metadata:    streamhub.StreamMetadata{},
			ContentType: "application/json",
		},
		Exp: streamhub.Message{
			ID:              "123",
			Stream:          "",
			Source:          "com.streamhub",
			SpecVersion:     streamhub.CloudEventsSpecVersion,
			Type:            "",
			Data:            []byte("foo"),
			DataContentType: "application/json",
			DataSchema:      "",
			Time:            "",
		},
	},
	{
		In: streamhub.NewMessageArgs{
			Data:   []byte("foo"),
			ID:     "123",
			Source: "com.streamhub",
			Metadata: streamhub.StreamMetadata{
				Stream:           "foo-stream",
				SchemaDefinition: "foo_stream",
				SchemaVersion:    0,
			},
			ContentType: "application/json",
		},
		Exp: streamhub.Message{
			ID:              "123",
			Stream:          "foo-stream",
			Source:          "com.streamhub",
			SpecVersion:     streamhub.CloudEventsSpecVersion,
			Type:            "com.streamhub.foo-stream.v0",
			Data:            []byte("foo"),
			DataContentType: "application/json",
			DataSchema:      "foo_stream#0",
		},
	},
	{
		In: streamhub.NewMessageArgs{
			Data:   []byte("foo"),
			ID:     "123",
			Source: "com.streamhub",
			Metadata: streamhub.StreamMetadata{
				Stream:           "foo-stream",
				SchemaDefinition: "foo_stream",
				SchemaVersion:    4,
			},
			ContentType: "application/json",
		},
		Exp: streamhub.Message{
			ID:              "123",
			Stream:          "foo-stream",
			Source:          "com.streamhub",
			SpecVersion:     streamhub.CloudEventsSpecVersion,
			Type:            "com.streamhub.foo-stream.v4",
			Data:            []byte("foo"),
			DataContentType: "application/json",
			DataSchema:      "foo_stream#4",
		},
	},
}

func TestNewMessage(t *testing.T) {
	for _, tt := range newMessageSuite {
		t.Run("", func(t *testing.T) {
			exp := streamhub.NewMessage(tt.In)
			assert.Equal(t, tt.Exp.ID, exp.ID)
			assert.Equal(t, tt.Exp.Stream, exp.Stream)
			assert.Equal(t, tt.Exp.Source, exp.Source)
			assert.Equal(t, streamhub.CloudEventsSpecVersion, exp.SpecVersion)
			if tt.In.Source != "" && tt.In.Metadata.Stream != "" {
				assert.Equal(t, tt.Exp.Type, exp.Type)
			}
			assert.Equal(t, tt.Exp.Data, exp.Data)
			if tt.In.ContentType != "" {
				assert.Equal(t, tt.Exp.DataContentType, exp.DataContentType)
			}
			if tt.In.Metadata.SchemaDefinition != "" {
				assert.Equal(t, tt.Exp.DataSchema, exp.DataSchema)
			}
			assert.NotEmpty(t, exp.Time)
		})
	}
}

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
			SchemaVersion:        0,
			Data:                 []byte("foo"),
			ID:                   "",
			Source:               "",
			Stream:               "",
			SchemaDefinitionName: "",
			ContentType:          "",
		},
		Exp: streamhub.Message{
			Data: []byte("foo"),
		},
	},
	{
		In: streamhub.NewMessageArgs{
			SchemaVersion:        0,
			Data:                 []byte("foo"),
			ID:                   "123",
			Source:               "com.streamhub",
			Stream:               "",
			SchemaDefinitionName: "",
			ContentType:          "application/json",
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
			SchemaVersion:        0,
			Data:                 []byte("foo"),
			ID:                   "123",
			Source:               "com.streamhub",
			Stream:               "foo-stream",
			SchemaDefinitionName: "foo_stream",
			ContentType:          "application/json",
		},
		Exp: streamhub.Message{
			ID:              "123",
			Stream:          "foo-stream",
			Source:          "com.streamhub",
			SpecVersion:     streamhub.CloudEventsSpecVersion,
			Type:            "com.streamhub.foo-stream.v0",
			Data:            []byte("foo"),
			DataContentType: "application/json",
			DataSchema:      "foo_stream#v0",
		},
	},
	{
		In: streamhub.NewMessageArgs{
			SchemaVersion:        4,
			Data:                 []byte("foo"),
			ID:                   "123",
			Source:               "com.streamhub",
			Stream:               "foo-stream",
			SchemaDefinitionName: "foo_stream",
			ContentType:          "application/json",
		},
		Exp: streamhub.Message{
			ID:              "123",
			Stream:          "foo-stream",
			Source:          "com.streamhub",
			SpecVersion:     streamhub.CloudEventsSpecVersion,
			Type:            "com.streamhub.foo-stream.v4",
			Data:            []byte("foo"),
			DataContentType: "application/json",
			DataSchema:      "foo_stream#v4",
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
			if tt.In.Source != "" && tt.In.Stream != "" {
				assert.Equal(t, tt.Exp.Type, exp.Type)
			}
			assert.Equal(t, tt.Exp.Data, exp.Data)
			if tt.In.ContentType != "" {
				assert.Equal(t, tt.Exp.DataContentType, exp.DataContentType)
			}
			assert.Equal(t, tt.Exp.DataSchema, exp.DataSchema)
			assert.NotEmpty(t, exp.Time)
		})
	}
}

func BenchmarkNewMessage(b *testing.B) {
	data := []byte("hello there")
	for i := 0; i < b.N; i++ {
		b.ReportAllocs()
		_ = streamhub.NewMessage(streamhub.NewMessageArgs{
			SchemaVersion:        9,
			Data:                 data,
			ID:                   "1",
			Source:               "",
			Stream:               "bar-stream",
			SchemaDefinitionName: "",
			ContentType:          "",
		})
	}
}

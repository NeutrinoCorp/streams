package streams_test

import (
	"testing"

	"github.com/neutrinocorp/streams"
	"github.com/stretchr/testify/assert"
)

func TestNewMessage(t *testing.T) {
	tests := []struct {
		Name string
		In   streams.NewMessageArgs
		Exp  streams.Message
	}{
		{
			Name: "Empty",
			In:   streams.NewMessageArgs{},
			Exp:  streams.Message{},
		},
		{
			Name: "Data only",
			In: streams.NewMessageArgs{
				SchemaVersion:        0,
				Data:                 []byte("foo"),
				ID:                   "",
				Source:               "",
				Stream:               "",
				SchemaDefinitionName: "",
				ContentType:          "",
			},
			Exp: streams.Message{
				Data: []byte("foo"),
			},
		},
		{
			Name: "No stream and schema def",
			In: streams.NewMessageArgs{
				SchemaVersion:        0,
				Data:                 []byte("foo"),
				ID:                   "123",
				Source:               "com.streams",
				Stream:               "",
				SchemaDefinitionName: "",
				ContentType:          "application/json",
			},
			Exp: streams.Message{
				ID:              "123",
				Stream:          "",
				Source:          "com.streams",
				SpecVersion:     streams.CloudEventsSpecVersion,
				Type:            "",
				Data:            []byte("foo"),
				DataContentType: "application/json",
				DataSchema:      "",
				Timestamp:       "",
			},
		},
		{
			Name: "No schema ver",
			In: streams.NewMessageArgs{
				SchemaVersion:        0,
				Data:                 []byte("foo"),
				ID:                   "123",
				Source:               "com.streams",
				Stream:               "foo-stream",
				SchemaDefinitionName: "foo_stream",
				ContentType:          "application/json",
			},
			Exp: streams.Message{
				ID:                "123",
				Stream:            "foo-stream",
				Source:            "com.streams",
				SpecVersion:       streams.CloudEventsSpecVersion,
				Type:              "com.streams.foo-stream.v0",
				Data:              []byte("foo"),
				DataContentType:   "application/json",
				DataSchema:        "foo_stream",
				DataSchemaVersion: 0,
			},
		},
		{
			Name: "Type with source attached",
			In: streams.NewMessageArgs{
				SchemaVersion:        4,
				Data:                 []byte("foo"),
				ID:                   "123",
				Source:               "com.streams",
				Stream:               "foo-stream",
				SchemaDefinitionName: "foo_stream",
				ContentType:          "application/json",
			},
			Exp: streams.Message{
				ID:                "123",
				Stream:            "foo-stream",
				Source:            "com.streams",
				SpecVersion:       streams.CloudEventsSpecVersion,
				Type:              "com.streams.foo-stream.v0",
				Data:              []byte("foo"),
				DataContentType:   "application/json",
				DataSchema:        "foo_stream",
				DataSchemaVersion: 4,
			},
		},
		{
			Name: "Default type",
			In: streams.NewMessageArgs{
				SchemaVersion:        4,
				Data:                 []byte("foo"),
				ID:                   "123",
				Source:               "com.streams",
				Stream:               "foo-stream",
				SchemaDefinitionName: "foo_stream",
				ContentType:          "application/json",
				GroupName:            "foo-group",
			},
			Exp: streams.Message{
				ID:                "123",
				Stream:            "foo-stream",
				Source:            "com.streams",
				SpecVersion:       streams.CloudEventsSpecVersion,
				Type:              "com.streams.foo-stream.v0",
				Data:              []byte("foo"),
				DataContentType:   "application/json",
				DataSchema:        "foo_stream",
				DataSchemaVersion: 4,
				GroupName:         "foo-group",
			},
		},
		{
			Name: "Full no source attached to stream",
			In: streams.NewMessageArgs{
				SchemaVersion:        4,
				Data:                 []byte("foo"),
				ID:                   "123",
				Source:               "com.streams",
				Stream:               "foo-stream",
				SchemaDefinitionName: "foo_stream",
				ContentType:          "application/json",
				GroupName:            "foo-group",
				Subject:              "foo-sub",
			},
			Exp: streams.Message{
				ID:                "123",
				Stream:            "foo-stream",
				Source:            "com.streams",
				SpecVersion:       streams.CloudEventsSpecVersion,
				Type:              "com.streams.foo-stream.v0",
				Data:              []byte("foo"),
				DataContentType:   "application/json",
				DataSchema:        "foo_stream",
				DataSchemaVersion: 4,
				GroupName:         "foo-group",
				Subject:           "foo-sub",
			},
		},
		{
			Name: "Valid",
			In: streams.NewMessageArgs{
				SchemaVersion:        4,
				Data:                 []byte("foo"),
				ID:                   "123",
				Source:               "com.streams",
				Stream:               "com.streams.foo-stream",
				StreamVersion:        7,
				SchemaDefinitionName: "foo_stream",
				ContentType:          "application/json",
				GroupName:            "foo-group",
				Subject:              "foo-sub",
			},
			Exp: streams.Message{
				ID:                "123",
				Stream:            "com.streams.foo-stream",
				Source:            "com.streams",
				SpecVersion:       streams.CloudEventsSpecVersion,
				Type:              "com.streams.foo-stream.v7",
				Data:              []byte("foo"),
				DataContentType:   "application/json",
				DataSchema:        "foo_stream",
				DataSchemaVersion: 4,
				GroupName:         "foo-group",
				Subject:           "foo-sub",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.Name, func(t *testing.T) {
			exp := streams.NewMessage(tt.In)
			assert.Equal(t, tt.Exp.ID, exp.ID)
			assert.Equal(t, tt.Exp.Stream, exp.Stream)
			assert.Equal(t, tt.Exp.Source, exp.Source)
			assert.Equal(t, streams.CloudEventsSpecVersion, exp.SpecVersion)
			if tt.In.Source != "" && tt.In.Stream != "" {
				assert.Equal(t, tt.Exp.Type, exp.Type)
			}
			assert.Equal(t, tt.Exp.Data, exp.Data)
			if tt.In.ContentType != "" {
				assert.Equal(t, tt.Exp.DataContentType, exp.DataContentType)
			}
			assert.Equal(t, tt.Exp.DataSchema, exp.DataSchema)
			assert.Equal(t, tt.Exp.DataSchemaVersion, exp.DataSchemaVersion)
			assert.NotEmpty(t, exp.Timestamp)
		})
	}
}

func BenchmarkNewMessage(b *testing.B) {
	data := []byte("hello there")
	for i := 0; i < b.N; i++ {
		b.ReportAllocs()
		_ = streams.NewMessage(streams.NewMessageArgs{
			SchemaVersion:        9,
			Data:                 data,
			ID:                   "1",
			Source:               "com.streams",
			Stream:               "bar-stream",
			SchemaDefinitionName: "",
			ContentType:          "",
		})
	}
}

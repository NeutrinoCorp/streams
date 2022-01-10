package streamhub

import (
	"strconv"
	"strings"
	"time"
)

// CloudEventsSpecVersion the CloudEvents specification version used by streamhub
const CloudEventsSpecVersion = "1.0"

// Message is a unit of information which holds the primitive message (data) in binary format along multiple
// fields in order to preserve a schema definition within a stream pipeline.
//
// The schema is based on the Cloud Native Computing Foundation (CNCF)'s CloudEvents specification.
//
// For more information, please look: https://github.com/cloudevents/spec
type Message struct {
	ID          string `json:"id"`
	Stream      string `json:"stream"`
	Source      string `json:"source"`
	SpecVersion string `json:"specversion"`
	Type        string `json:"type"`
	Data        []byte `json:"data"`

	// Optional fields

	DataContentType string `json:"datacontenttype,omitempty"`
	DataSchema      string `json:"dataschema,omitempty"`
	Time            string `json:"time,omitempty"`
}

// NewMessageArgs arguments required by NewMessage function to operate.
type NewMessageArgs struct {
	Data        []byte
	ID          string
	Source      string
	Metadata    StreamMetadata
	ContentType string
}

// NewMessage allocates an immutable Message ready to be transported in a stream.
func NewMessage(args NewMessageArgs) Message {
	strSchemaVersion := strconv.Itoa(args.Metadata.SchemaVersion)
	return Message{
		ID:              args.ID,
		Stream:          args.Metadata.Stream,
		Source:          args.Source,
		SpecVersion:     CloudEventsSpecVersion,
		Type:            generateMessageType(args.Source, args.Metadata.Stream, strSchemaVersion),
		Data:            args.Data,
		DataContentType: args.ContentType,
		DataSchema:      args.Metadata.SchemaDefinition + "#" + strSchemaVersion,
		Time:            time.Now().UTC().Format(time.RFC3339),
	}
}

func generateMessageType(source, stream, version string) string {
	buff := strings.Builder{}
	if source != "" {
		buff.WriteString(source)
		buff.WriteString(".")
	}
	buff.WriteString(stream)
	if version != "" {
		buff.WriteString(".v")
		buff.WriteString(version)
	}
	return buff.String()
}

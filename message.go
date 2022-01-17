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

	// Streamhub fields
	CorrelationID string `json:"correlation_id"`
	CausationID   string `json:"causation_id"`
}

// NewMessageArgs arguments required by NewMessage function to operate.
type NewMessageArgs struct {
	SchemaVersion        int
	Data                 []byte
	ID                   string
	Source               string
	Stream               string
	SchemaDefinitionName string
	ContentType          string
}

// NewMessage allocates an immutable Message ready to be transported in a stream.
func NewMessage(args NewMessageArgs) Message {
	strSchemaVersion := strconv.Itoa(args.SchemaVersion)
	return Message{
		ID:              args.ID,
		Stream:          args.Stream,
		Source:          args.Source,
		SpecVersion:     CloudEventsSpecVersion,
		Type:            generateMessageType(args.Source, args.Stream, strSchemaVersion),
		Data:            args.Data,
		DataContentType: args.ContentType,
		DataSchema:      generateMessageSchema(args.SchemaDefinitionName, strSchemaVersion),
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

func generateMessageSchema(schemaDef, version string) string {
	if schemaDef == "" {
		return ""
	}
	return schemaDef + "#v" + version
}

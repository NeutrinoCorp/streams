package streams

import (
	"strconv"
	"strings"
	"time"
)

// CloudEventsSpecVersion the CloudEvents specification version used by streams
const CloudEventsSpecVersion = "1.0"

// Message is a unit of information which holds the primitive message (data) in binary format along multiple
// fields in order to preserve a schema definition within a stream pipeline.
//
// The schema is based on the Cloud Native Computing Foundation (CNCF)'s CloudEvents specification.
//
// For more information, please look: https://github.com/cloudevents/spec
type Message struct {
	// Stream name of destination stream (aka. topic)
	Stream string `json:"stream"`
	// StreamVersion destination stream major version. Useful when non-backwards compatible schema update is desired.
	StreamVersion int    `json:"stream_version"`
	ID            string `json:"id"`
	Source        string `json:"source"`
	SpecVersion   string `json:"specversion"`
	Type          string `json:"type"`
	Data          []byte `json:"data"`

	// Optional fields

	DataContentType   string `json:"datacontenttype,omitempty"`
	DataSchema        string `json:"dataschema,omitempty"`
	DataSchemaVersion int    `json:"dataschemaversion,omitempty"`
	Timestamp         string `json:"time,omitempty"`
	Subject           string `json:"subject,omitempty"`

	// Streamhub fields
	CorrelationID string `json:"correlation_id"`
	CausationID   string `json:"causation_id"`

	// consumer-only fields

	// DecodedData data decoded using unmarshalling ReaderBehaviour component. This field is ONLY available for usage
	// from ReaderNode(s).
	DecodedData interface{} `json:"-"`
	// GroupName name of the reader group (aka. consumer group). This field is ONLY available for usage
	// from ReaderNode(s).
	GroupName string `json:"-"`
}

// NewMessageArgs arguments required by NewMessage function to operate.
type NewMessageArgs struct {
	SchemaVersion        int
	Data                 []byte
	ID                   string
	Source               string
	Stream               string
	StreamVersion        int
	SchemaDefinitionName string
	ContentType          string
	GroupName            string
	Subject              string
}

// NewMessage allocates an immutable Message ready to be transported in a stream.
func NewMessage(args NewMessageArgs) Message {
	return Message{
		ID:                args.ID,
		Stream:            args.Stream,
		StreamVersion:     args.StreamVersion,
		Source:            args.Source,
		SpecVersion:       CloudEventsSpecVersion,
		Type:              newMessageType(args.Source, args.Stream, strconv.Itoa(args.StreamVersion)),
		Data:              args.Data,
		DataContentType:   args.ContentType,
		DataSchema:        args.SchemaDefinitionName,
		DataSchemaVersion: args.SchemaVersion,
		Timestamp:         time.Now().UTC().Format(time.RFC3339),
		Subject:           args.Subject,
		GroupName:         args.GroupName,
	}
}

func newMessageType(source, stream, version string) string {
	buff := strings.Builder{}
	sourceHasPrefix := strings.HasPrefix(stream, source)
	buffSize := calculateMessageTypeBufferSize(len(source), len(stream), len(version), !sourceHasPrefix)
	buff.Grow(buffSize)
	if source != "" && !sourceHasPrefix {
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

func calculateMessageTypeBufferSize(source, stream, version int, useSource bool) int {
	buffSize := stream + version
	if version > 0 {
		// version will add ".v" extra chars to buffer
		buffSize += 2
	}
	if source > 0 && useSource {
		// source will add '.' extra char to buffer
		buffSize += source + 1
	}
	return buffSize
}

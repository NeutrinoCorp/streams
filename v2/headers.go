package streams

const (
	HeaderMessageID     = "streams-message-id"
	HeaderCorrelationID = "streams-correlation-id"
	HeaderCausationID   = "streams-causation-id"
	HeaderStreamName    = "streams-stream-name"
	HeaderContentType   = "streams-content-type"
	HeaderSchemaURL     = "streams-schema-url"
)

var HeaderSet = map[string]struct{}{
	HeaderMessageID:     {},
	HeaderCorrelationID: {},
	HeaderCausationID:   {},
	HeaderStreamName:    {},
	HeaderContentType:   {},
	HeaderSchemaURL:     {},
}

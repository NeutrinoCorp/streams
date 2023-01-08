package streams

type Message struct {
	ID              string            `json:"message_id"`
	CorrelationID   string            `json:"correlation_id"`
	CausationID     string            `json:"causation_id"`
	StreamName      string            `json:"stream_name"`
	ContentType     string            `json:"content_type"`
	SchemaURL       string            `json:"schema_url"`
	Data            []byte            `json:"data"`
	TimestampMillis int64             `json:"timestamp_millis"`
	Headers         map[string]string `json:"headers"`

	DecodedData interface{} `json:"-"`
}

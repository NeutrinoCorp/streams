package streams_test

// since streams.Message schema will evolve over time, this structure (fake) serves as an immutable -or
// at least not subjective to change very often- structure to avoid tests from breaking.
type fakeMessage struct {
	ID             string            `json:"message_id"`
	CorrelationID  string            `json:"correlation_id"`
	CausationID    string            `json:"causation_id"`
	StreamName     string            `json:"stream_name"`
	StreamVersion  int               `json:"stream_version"`
	StreamFullName string            `json:"stream_full_name"`
	SchemaURL      string            `json:"schema_url"`
	Data           []byte            `json:"data"`
	Metadata       map[string]string `json:"metadata"`
}

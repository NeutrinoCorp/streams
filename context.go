package streamhub

import "context"

// MessageContextKey is the streamhub context key to inject data into transport messages.
type MessageContextKey string

const (
	// ContextCorrelationID is the main trace of a stream processing. Once generated, it MUST NOT be generated again
	// to keep track of the process from the beginning.
	ContextCorrelationID MessageContextKey = "shub-correlation-id"
	// ContextCausationID is reference of the last message processed. This helps to know a direct relation between
	// a new process and the past one.
	ContextCausationID MessageContextKey = "shub-causation-id"
)

// InjectMessageCorrelationID injects the correlation id from the given context if available. If not, it will use the
// message id as fallback.
func InjectMessageCorrelationID(ctx context.Context, messageID string) string {
	if ctx == nil {
		return messageID
	} else if correlation, ok := ctx.Value(ContextCorrelationID).(MessageContextKey); ok {
		return string(correlation)
	}

	return messageID
}

// InjectMessageCausationID injects the causation id from the given context if available. If not, it will use the
// message id as fallback.
func InjectMessageCausationID(ctx context.Context, messageID string) string {
	if ctx == nil {
		return messageID
	} else if causation, ok := ctx.Value(ContextCausationID).(MessageContextKey); ok {
		return string(causation)
	}

	return messageID
}

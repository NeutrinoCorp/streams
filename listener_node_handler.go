package streamhub

import (
	"context"
)

// ListenerNodeHandler is the handling function for each incoming message from a stream used by streamhub's
// ListenerDriver(s).
//
// The base handler contains retry backoff mechanisms, correlation and context id injection,
type ListenerNodeHandler func(context.Context, Message) error

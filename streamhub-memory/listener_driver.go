package streamhub_memory

import (
	"context"

	"github.com/neutrinocorp/streamhub"
)

// ListenerDriver is the streamhub.ListenerDriver in-memory implementation
type ListenerDriver struct {
	b *Bus
}

var _ streamhub.ListenerDriver = &ListenerDriver{}

// NewListener allocates a new ListenerDriver ready to interact with the given Bus
func NewListener(b *Bus) *ListenerDriver {
	return &ListenerDriver{b: b}
}

// ExecuteTask starts the stream-listening job using the internal in-memory Bus
func (l *ListenerDriver) ExecuteTask(ctx context.Context, node *streamhub.ListenerNode) error {
	l.b.registerHandler(node)
	l.b.start(ctx)
	return nil
}

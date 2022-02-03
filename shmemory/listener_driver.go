package shmemory

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
func (l *ListenerDriver) ExecuteTask(ctx context.Context, t streamhub.ListenerTask) error {
	l.b.registerHandler(t)
	l.b.start(ctx)
	return nil
}

package shmemory

import (
	"context"

	"github.com/neutrinocorp/streamhub"
)

type ListenerDriver struct {
	b *Bus
}

var _ streamhub.ListenerDriver = &ListenerDriver{}

func NewListener(b *Bus) *ListenerDriver {
	return &ListenerDriver{b: b}
}

func (l *ListenerDriver) ExecuteTask(ctx context.Context, t streamhub.ListenerTask) error {
	l.b.registerHandler(t)
	l.b.start(ctx)
	return nil
}

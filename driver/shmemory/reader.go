package shmemory

import (
	"context"

	"github.com/neutrinocorp/streamhub"
)

// Reader is the streamhub.Reader in-memory implementation
type Reader struct {
	b *Bus
}

var _ streamhub.Reader = &Reader{}

// NewReader allocates a new Reader ready to interact with the given Bus
func NewReader(b *Bus) *Reader {
	return &Reader{b: b}
}

// ExecuteTask starts the stream-listening job using the internal in-memory Bus
func (l *Reader) ExecuteTask(ctx context.Context, t streamhub.ReaderTask) error {
	l.b.registerHandler(t)
	l.b.start(ctx)
	return nil
}

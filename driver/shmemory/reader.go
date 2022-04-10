package shmemory

import (
	"context"

	"github.com/neutrinocorp/streams"
)

// Reader is the streams.Reader in-memory implementation
type Reader struct {
	b *Bus
}

var _ streams.Reader = &Reader{}

// NewReader allocates a new Reader ready to interact with the given Bus
func NewReader(b *Bus) *Reader {
	return &Reader{b: b}
}

// ExecuteTask starts the stream-listening job using the internal in-memory Bus
func (l *Reader) ExecuteTask(ctx context.Context, t streams.ReaderTask) error {
	l.b.registerHandler(t)
	l.b.start(ctx)
	return nil
}

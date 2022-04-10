package shmemory

import (
	"context"
	"errors"

	"github.com/neutrinocorp/streamhub"
)

// ErrBusNotStarted The in-memory bus has not been started
var ErrBusNotStarted = errors.New("streamhub: In-memory bus has not been started")

// Writer is the streamhub.Writer in-memory implementation
type Writer struct {
	b *Bus
}

var _ streamhub.Writer = &Writer{}

// NewWriter allocates a new Writer ready to be used with the given Bus
func NewWriter(b *Bus) *Writer {
	return &Writer{b: b}
}

// Write pushes the given message into the internal in-memory Bus
func (p *Writer) Write(ctx context.Context, message streamhub.Message) error {
	return p.b.write(ctx, message)
}

// WriteBatch pushes the given set of messages into the internal in-memory Bus
func (p *Writer) WriteBatch(ctx context.Context, messages ...streamhub.Message) (published uint32, err error) {
	for _, msg := range messages {
		if errPub := p.b.write(ctx, msg); errPub != nil {
			err = errPub
			continue
		}
		published++
	}
	return
}

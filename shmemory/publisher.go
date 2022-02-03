package shmemory

import (
	"context"

	"github.com/neutrinocorp/streamhub"
)

// Publisher is the streamhub.Publisher in-memory implementation
type Publisher struct {
	b *Bus
}

var _ streamhub.Publisher = &Publisher{}

// NewPublisher allocates a new Publisher ready to be used with the given Bus
func NewPublisher(b *Bus) *Publisher {
	return &Publisher{b: b}
}

// Publish pushes the given message into the internal in-memory Bus
func (p *Publisher) Publish(_ context.Context, message streamhub.Message) error {
	p.b.messageBuffer <- message
	return nil
}

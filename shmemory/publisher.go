package shmemory

import (
	"context"

	"github.com/neutrinocorp/streamhub"
)

type Publisher struct {
	b *Bus
}

var _ streamhub.Publisher = &Publisher{}

func NewPublisher(b *Bus) *Publisher {
	return &Publisher{b: b}
}

func (p *Publisher) Publish(_ context.Context, message streamhub.Message) error {
	p.b.messageBuffer <- message
	return nil
}

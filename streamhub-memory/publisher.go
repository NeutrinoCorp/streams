package streamhub_memory

import (
	"context"
	"errors"

	"github.com/neutrinocorp/streamhub"
)

// ErrBusNotStarted The in-memory bus has not been started
var ErrBusNotStarted = errors.New("streamhub: In-memory bus has not been started")

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
func (p *Publisher) Publish(ctx context.Context, message streamhub.Message) error {
	return p.b.publish(ctx, message)
}

// PublishBatch pushes the given set of messages into the internal in-memory Bus
func (p *Publisher) PublishBatch(ctx context.Context, messages ...streamhub.Message) (err error) {
	for _, msg := range messages {
		if errPub := p.b.publish(ctx, msg); errPub != nil {
			err = errPub
		}
	}
	return
}

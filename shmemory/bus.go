package shmemory

import (
	"context"

	"github.com/neutrinocorp/streamhub"
)

// Bus is an in-memory message broker to enable interactions between publishers and stream-listeners
type Bus struct {
	messageBuffer chan streamhub.Message
	// key: Stream name | value: List of handlers
	messageHandlers map[string][]streamhub.ListenerTask

	startedBus bool
}

// NewBus allocates a new Bus ready to be used
func NewBus() *Bus {
	return &Bus{
		messageBuffer:   make(chan streamhub.Message),
		messageHandlers: map[string][]streamhub.ListenerTask{},
		startedBus:      false,
	}
}

func (b *Bus) registerHandler(task streamhub.ListenerTask) {
	handlers, ok := b.messageHandlers[task.Stream]
	if !ok {
		handlers = make([]streamhub.ListenerTask, 0)
	}

	handlers = append(handlers, task)
	b.messageHandlers[task.Stream] = handlers
}

// start listen to the underlying message buffer queue that will be later used by publishers.
// Inner operations will schedule stream-listeners if subscribed to the arrived message stream.
//
// In addition, the bus contains a very basic boolean lock to avoid multiple message buffer listening jobs running concurrently.
func (b *Bus) start(ctx context.Context) {
	if b.startedBus {
		return
	}
	go func() {
		for msg := range b.messageBuffer {
			tasks := b.messageHandlers[msg.Stream]
			for _, t := range tasks {
				go func(t streamhub.ListenerTask) {
					scopedCtx, cancel := context.WithTimeout(ctx, t.Timeout)
					defer cancel()
					_ = t.HandlerFunc(scopedCtx, msg)
				}(t)
			}
		}
	}()
	go func() {
		for {
			select {
			case <-ctx.Done():
				close(b.messageBuffer)
				return
			default:
			}
		}
	}()
	b.startedBus = true
}

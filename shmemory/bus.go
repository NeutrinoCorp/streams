package shmemory

import (
	"context"

	"github.com/neutrinocorp/streamhub"
)

type Bus struct {
	messageBuffer   chan streamhub.Message
	messageHandlers map[string][]streamhub.ListenerTask

	startedBus bool
}

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

			select {
			case <-ctx.Done():
				return
			default:
			}
		}
	}()
	b.startedBus = true
}

package shmemory

import (
	"context"

	"github.com/neutrinocorp/streams"
)

// Bus is an in-memory message broker to enable interactions between publishers and stream-listeners
type Bus struct {
	messageBuffer chan streams.Message
	// key: Stream name | value: List of handlers
	messageHandlers map[string][]streams.ReaderTask

	startedBus    bool
	maxGoroutines int
}

// NewBus allocates a new Bus ready to be used
func NewBus(maxGoroutines int) *Bus {
	if maxGoroutines <= 0 {
		maxGoroutines = 100
	}
	return &Bus{
		messageBuffer:   make(chan streams.Message),
		messageHandlers: map[string][]streams.ReaderTask{},
		startedBus:      false,
		maxGoroutines:   maxGoroutines,
	}
}

func (b *Bus) registerHandler(task streams.ReaderTask) {
	handlers, ok := b.messageHandlers[task.Stream]
	if !ok {
		handlers = make([]streams.ReaderTask, 0)
	}

	handlers = append(handlers, task)
	b.messageHandlers[task.Stream] = handlers
}

func (b *Bus) write(_ context.Context, message streams.Message) error {
	if !b.startedBus {
		return ErrBusNotStarted
	}
	b.messageBuffer <- message
	return nil
}

// start listen to the underlying message buffer queue that will be later used by publishers.
// Inner operations will schedule stream-listeners if subscribed to the arrived message stream.
//
// In addition, the bus contains a very basic boolean lock to avoid multiple message buffer listening jobs running
// concurrently.
func (b *Bus) start(ctx context.Context) {
	if b.startedBus {
		return
	}
	go func() {
		sem := make(chan struct{}, b.maxGoroutines)
		for msg := range b.messageBuffer {
			select {
			case sem <- struct{}{}:
			}
			for _, t := range b.messageHandlers[msg.Stream] {
				go func(task streams.ReaderTask, message streams.Message) {
					scopedCtx, cancel := context.WithTimeout(ctx, task.Timeout)
					defer cancel()
					defer func() { <-sem }()
					_ = task.HandlerFunc(scopedCtx, message)
				}(t, msg)
			}
		}
		// acquire semaphore
		for n := 0; n < b.maxGoroutines; n++ {
			sem <- struct{}{}
		}
	}()
	go func() {
		select {
		case <-ctx.Done():
			close(b.messageBuffer)
			return
		}
	}()
	b.startedBus = true
}

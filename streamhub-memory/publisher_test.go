package streamhub_memory_test

import (
	"context"
	"runtime"
	"testing"
	"time"

	"github.com/neutrinocorp/streamhub"
	streamhub_memory "github.com/neutrinocorp/streamhub/streamhub-memory"
	"github.com/stretchr/testify/assert"
)

func TestPublisher_Publish(t *testing.T) {
	assert.Equal(t, 2, runtime.NumGoroutine())

	bus := streamhub_memory.NewBus(0)
	p := streamhub_memory.NewPublisher(bus)
	err := p.Publish(context.Background(), streamhub.Message{
		Stream: "foo-stream",
	})
	assert.ErrorIs(t, err, streamhub_memory.ErrBusNotStarted)
	assert.Equal(t, 2, runtime.NumGoroutine())

	d := streamhub_memory.NewListener(bus)
	baseCtx, cancel := context.WithTimeout(context.Background(), time.Millisecond*100)
	defer cancel()
	err = d.ExecuteTask(baseCtx, streamhub.ListenerTask{
		Stream: "foo-stream",
		HandlerFunc: func(ctx context.Context, message streamhub.Message) error {
			assert.Equal(t, "foo-stream", message.Stream)
			return nil
		},
		Group:         "",
		Configuration: nil,
		Timeout:       0,
	})
	assert.NoError(t, err)
	err = p.Publish(context.Background(), streamhub.Message{
		Stream: "foo-stream",
	})
	assert.NoError(t, err)
	// in-memory message bus adds two go routines, one for message buffer subscription and another for graceful shutdown
	assert.LessOrEqual(t, 4, runtime.NumGoroutine())
	time.Sleep(time.Millisecond * 200)
	// ensure goroutines were de-scheduled
	assert.Equal(t, 2, runtime.NumGoroutine())
}

func TestPublisher_PublishBatch(t *testing.T) {
	assert.Equal(t, 2, runtime.NumGoroutine())

	bus := streamhub_memory.NewBus(0)
	p := streamhub_memory.NewPublisher(bus)
	err := p.PublishBatch(context.Background(), streamhub.Message{
		Stream: "foo-stream",
	})
	assert.ErrorIs(t, err, streamhub_memory.ErrBusNotStarted)
	assert.Equal(t, 2, runtime.NumGoroutine())

	d := streamhub_memory.NewListener(bus)
	baseCtx, cancel := context.WithTimeout(context.Background(), time.Millisecond*100)
	defer cancel()
	err = d.ExecuteTask(baseCtx, streamhub.ListenerTask{
		Stream: "foo-stream",
		HandlerFunc: func(ctx context.Context, message streamhub.Message) error {
			assert.Equal(t, "foo-stream", message.Stream)
			return nil
		},
		Group:         "",
		Configuration: nil,
		Timeout:       0,
	})
	assert.NoError(t, err)
	err = p.PublishBatch(context.Background(), streamhub.Message{
		Stream: "foo-stream",
	}, streamhub.Message{
		Stream: "foo-stream",
	})
	assert.NoError(t, err)
	// in-memory message bus adds two go routines, one for message buffer subscription and another for graceful shutdown
	assert.LessOrEqual(t, 4, runtime.NumGoroutine())
	time.Sleep(time.Millisecond * 200)
	// ensure goroutines were de-scheduled
	assert.Equal(t, 2, runtime.NumGoroutine())
}

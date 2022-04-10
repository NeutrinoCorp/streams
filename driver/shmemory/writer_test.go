package shmemory_test

import (
	"context"
	"runtime"
	"testing"
	"time"

	"github.com/neutrinocorp/streamhub"
	"github.com/neutrinocorp/streamhub/driver/shmemory"
	"github.com/stretchr/testify/assert"
)

func TestPublisher_Publish(t *testing.T) {
	assert.Equal(t, 2, runtime.NumGoroutine())

	bus := shmemory.NewBus(0)
	p := shmemory.NewWriter(bus)
	err := p.Write(context.Background(), streamhub.Message{
		Stream: "foo-stream",
	})
	assert.ErrorIs(t, err, shmemory.ErrBusNotStarted)
	assert.Equal(t, 2, runtime.NumGoroutine())

	d := shmemory.NewReader(bus)
	baseCtx, cancel := context.WithTimeout(context.Background(), time.Millisecond*100)
	defer cancel()
	err = d.ExecuteTask(baseCtx, streamhub.ReaderTask{
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
	err = p.Write(context.Background(), streamhub.Message{
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

	bus := shmemory.NewBus(0)
	p := shmemory.NewWriter(bus)
	_, err := p.WriteBatch(context.Background(), streamhub.Message{
		Stream: "foo-stream",
	})
	assert.ErrorIs(t, err, shmemory.ErrBusNotStarted)
	assert.Equal(t, 2, runtime.NumGoroutine())

	d := shmemory.NewReader(bus)
	baseCtx, cancel := context.WithTimeout(context.Background(), time.Millisecond*100)
	defer cancel()
	err = d.ExecuteTask(baseCtx, streamhub.ReaderTask{
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
	_, err = p.WriteBatch(context.Background(), streamhub.Message{
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

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

func TestReader_ExecuteTask(t *testing.T) {
	assert.Equal(t, 2, runtime.NumGoroutine())

	d := shmemory.NewReader(shmemory.NewBus(0))
	baseCtx, cancel := context.WithTimeout(context.Background(), time.Millisecond)
	defer cancel()
	err := d.ExecuteTask(baseCtx, streamhub.ReaderTask{
		Stream:        "",
		HandlerFunc:   nil,
		Group:         "",
		Configuration: nil,
		Timeout:       0,
	})
	assert.NoError(t, err)
	// in-memory message bus adds two go routines, one for message buffer subscription and another for graceful shutdown
	assert.Equal(t, 4, runtime.NumGoroutine())
	time.Sleep(time.Millisecond * 100)
	// ensure goroutines were de-scheduled
	assert.Equal(t, 2, runtime.NumGoroutine())
}

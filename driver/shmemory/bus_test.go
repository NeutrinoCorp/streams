package shmemory

import (
	"context"
	"runtime"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestBus_Start(t *testing.T) {
	assert.Equal(t, 2, runtime.NumGoroutine())

	b := NewBus(0)
	baseCtx, cancel := context.WithTimeout(context.Background(), time.Millisecond)
	defer cancel()
	b.start(baseCtx)
	assert.Equal(t, 4, runtime.NumGoroutine())
	// ensure message buffer listening task runs once
	b.start(baseCtx)
	assert.Equal(t, 4, runtime.NumGoroutine())
	time.Sleep(time.Millisecond * 100)
	assert.Equal(t, 2, runtime.NumGoroutine())
}

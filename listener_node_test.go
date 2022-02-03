package streamhub

import (
	"context"
	"runtime"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestListenerNode_Start(t *testing.T) {
	baseCtx, cancel := context.WithTimeout(context.Background(), time.Millisecond)
	defer cancel()
	concurrency := 10
	node := listenerNode{
		Stream:           "foo",
		ConcurrencyLevel: concurrency,
		ListenerDriver:   listenerDriverNoopLoop{},
	}
	node.start(baseCtx)
	assert.Equal(t, 12, runtime.NumGoroutine())
	time.Sleep(time.Millisecond * 10)
	assert.Equal(t, 2, runtime.NumGoroutine())

	node = listenerNode{
		Stream:           "foo",
		ConcurrencyLevel: 1,
		ListenerDriver:   listenerDriverNoop{},
	}
	node.start(baseCtx)
	assert.Equal(t, 3, runtime.NumGoroutine())
	time.Sleep(time.Millisecond)
	assert.Equal(t, 2, runtime.NumGoroutine())
}

func BenchmarkListenerNode_Start(b *testing.B) {
	baseCtx, cancel := context.WithCancel(context.Background())
	defer cancel()
	concurrency := 1
	node := listenerNode{
		Stream:           "foo",
		ConcurrencyLevel: concurrency,
		ListenerDriver:   listenerDriverNoop{},
	}
	for i := 0; i < b.N; i++ {
		b.ReportAllocs()
		node.start(baseCtx)
	}
}

package streamhub

import (
	"context"
	"runtime"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestListenerSupervisor_ForkNode(t *testing.T) {
	h := &Hub{}
	sv := newListenerSupervisor(h, WithGroup("bar-queue"))
	sv.forkNode("")
	assert.Equal(t, 0, len(sv.listenerRegistry))
	sv.forkNode("foo")
	assert.Equal(t, DefaultConcurrencyLevel, sv.listenerRegistry[0].ConcurrencyLevel)
	assert.Equal(t, DefaultRetryInitialInterval, sv.listenerRegistry[0].RetryInitialInterval)
	assert.Equal(t, DefaultRetryMaxInterval, sv.listenerRegistry[0].RetryMaxInterval)
	assert.Equal(t, DefaultRetryTimeout, sv.listenerRegistry[0].RetryTimeout)
	assert.Equal(t, "bar-queue", sv.listenerRegistry[0].Group)

	sv.forkNode("baz")
	assert.Equal(t, "bar-queue", sv.listenerRegistry[1].Group)

	sv.forkNode("foobar", WithGroup("barbaz-queue"), WithListenerFunc(func(ctx context.Context, message Message) error {
		return nil
	}))
	assert.Equal(t, "barbaz-queue", sv.listenerRegistry[2].Group)
	assert.NotNil(t, sv.listenerRegistry[2].HandlerFunc)
}

func BenchmarkListenerSupervisor_ForkNode(b *testing.B) {
	h := &Hub{}
	sv := newListenerSupervisor(h, WithGroup("bar-queue"), WithConcurrencyLevel(10))
	var handler ListenerFunc
	handler = func(_ context.Context, _ Message) error {
		return nil
	}
	for i := 0; i < b.N; i++ {
		b.ReportAllocs()
		sv.forkNode("baz", WithListenerFunc(handler))
	}
}

func TestListenerSupervisor_StartNodes(t *testing.T) {
	baseCtx, cancel := context.WithTimeout(context.Background(), time.Millisecond)
	defer cancel()

	h := NewHub()
	sv := newListenerSupervisor(h, WithDriver(listenerDriverNoop{}), WithListenerFunc(func(ctx context.Context, message Message) error {
		return nil
	}))
	sv.forkNode("foo")
	sv.forkNode("foo")
	sv.startNodes(baseCtx)

	assert.Equal(t, 4, runtime.NumGoroutine())
	time.Sleep(time.Millisecond * 100)
	assert.Equal(t, 2, runtime.NumGoroutine())
}

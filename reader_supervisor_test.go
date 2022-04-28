package streams

import (
	"context"
	"runtime"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestReaderSupervisor_ForkNode(t *testing.T) {
	h := NewHub(WithReaderBaseOptions(WithGroup("bar-queue")))
	sv := newReaderSupervisor(h)
	sv.forkNode("")
	assert.Equal(t, 0, len(sv.readerRegistry))
	sv.forkNode("foo")
	itemInterface, _ := sv.readerRegistry["foo"].Get(0)
	item := itemInterface.(ReaderNode)
	assert.Equal(t, DefaultConcurrencyLevel, item.ConcurrencyLevel)
	assert.Equal(t, DefaultRetryInitialInterval, item.RetryInitialInterval)
	assert.Equal(t, DefaultRetryMaxInterval, item.RetryMaxInterval)
	assert.Equal(t, DefaultRetryTimeout, item.RetryTimeout)
	assert.Equal(t, "bar-queue", item.Group)

	sv.forkNode("baz")
	itemInterface, _ = sv.readerRegistry["baz"].Get(0)
	item = itemInterface.(ReaderNode)
	assert.Equal(t, "bar-queue", item.Group)

	sv.forkNode("foobar", WithGroup("barbaz-queue"), WithHandlerFunc(func(ctx context.Context, message Message) error {
		return nil
	}))
	itemInterface, _ = sv.readerRegistry["foobar"].Get(0)
	item = itemInterface.(ReaderNode)
	assert.Equal(t, "barbaz-queue", item.Group)
	assert.NotNil(t, item.HandlerFunc)
}

func BenchmarkReaderSupervisor_ForkNode(b *testing.B) {
	h := NewHub(WithReaderBaseOptions(WithGroup("bar-queue"), WithConcurrencyLevel(10)))
	sv := newReaderSupervisor(h)
	var handler ReaderHandleFunc
	handler = func(_ context.Context, _ Message) error {
		return nil
	}
	for i := 0; i < b.N; i++ {
		b.ReportAllocs()
		sv.forkNode("baz", WithHandlerFunc(handler))
	}
}

func TestReaderSupervisor_StartNodes(t *testing.T) {
	baseCtx, cancel := context.WithTimeout(context.Background(), time.Millisecond)
	defer cancel()

	h := NewHub(WithReaderBaseOptions(WithDriver(readerNoop{}), WithHandlerFunc(func(ctx context.Context, message Message) error {
		return nil
	})))
	sv := newReaderSupervisor(h)
	sv.forkNode("foo")
	sv.forkNode("foo")
	sv.startNodes(baseCtx)

	assert.Equal(t, 4, runtime.NumGoroutine())
	time.Sleep(time.Millisecond * 100)
	assert.Equal(t, 2, runtime.NumGoroutine())
}

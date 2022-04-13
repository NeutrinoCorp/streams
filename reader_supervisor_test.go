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
	assert.Equal(t, DefaultConcurrencyLevel, sv.readerRegistry["foo"][0].ConcurrencyLevel)
	assert.Equal(t, DefaultRetryInitialInterval, sv.readerRegistry["foo"][0].RetryInitialInterval)
	assert.Equal(t, DefaultRetryMaxInterval, sv.readerRegistry["foo"][0].RetryMaxInterval)
	assert.Equal(t, DefaultRetryTimeout, sv.readerRegistry["foo"][0].RetryTimeout)
	assert.Equal(t, "bar-queue", sv.readerRegistry["foo"][0].Group)

	sv.forkNode("baz")
	assert.Equal(t, "bar-queue", sv.readerRegistry["baz"][0].Group)

	sv.forkNode("foobar", WithGroup("barbaz-queue"), WithHandlerFunc(func(ctx context.Context, message Message) error {
		return nil
	}))
	assert.Equal(t, "barbaz-queue", sv.readerRegistry["foobar"][0].Group)
	assert.NotNil(t, sv.readerRegistry["foobar"][0].HandlerFunc)
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

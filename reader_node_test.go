package streamhub

import (
	"context"
	"runtime"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestReaderNode_Start(t *testing.T) {
	baseCtx, cancel := context.WithTimeout(context.Background(), time.Millisecond)
	defer cancel()
	concurrency := 10
	node := ReaderNode{
		Stream:           "foo",
		ConcurrencyLevel: concurrency,
		Reader:           readerNoopLoop{},
	}
	node.start(baseCtx)
	assert.GreaterOrEqual(t, 12, runtime.NumGoroutine())
	time.Sleep(time.Millisecond * 100)
	assert.Equal(t, 2, runtime.NumGoroutine())

	baseCtx2, cancelCtx2 := context.WithTimeout(context.Background(), time.Millisecond)
	defer cancelCtx2()
	node = ReaderNode{
		Stream:           "foo",
		ConcurrencyLevel: 1,
		Reader:           readerNoop{},
	}
	node.start(baseCtx2)
	assert.Equal(t, 3, runtime.NumGoroutine())
	time.Sleep(time.Millisecond * 100)
	assert.Equal(t, 2, runtime.NumGoroutine())
}

func BenchmarkReaderNode_Start(b *testing.B) {
	baseCtx, cancel := context.WithCancel(context.Background())
	defer cancel()
	concurrency := 1
	node := ReaderNode{
		Stream:           "foo",
		ConcurrencyLevel: concurrency,
		Reader:           readerNoop{},
	}
	for i := 0; i < b.N; i++ {
		b.ReportAllocs()
		node.start(baseCtx)
	}
}

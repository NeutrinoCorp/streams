package streams_test

import (
	"context"
	"errors"
	"hash"

	"github.com/neutrinocorp/streams"
)

type listenerDriverNoop struct{}

var _ streams.Reader = listenerDriverNoop{}

// ExecuteTask the no-operation implementation of Reader
func (l listenerDriverNoop) ExecuteTask(_ context.Context, _ streams.ReaderTask) error {
	return nil
}

type listenerDriverNoopGoroutine struct{}

var _ streams.Reader = listenerDriverNoopGoroutine{}

// ExecuteTask the no-operation implementation of Reader inside a goroutine
func (l listenerDriverNoopGoroutine) ExecuteTask(_ context.Context, _ streams.ReaderTask) error {
	go func() {}()
	return nil
}

var hashing64GenericError = errors.New("failed at hashing64")

type hashing64AlgorithmFailingNoop struct{}

var _ hash.Hash64 = hashing64AlgorithmFailingNoop{}

func (h hashing64AlgorithmFailingNoop) Write(_ []byte) (n int, err error) {
	return 0, hashing64GenericError
}

func (h hashing64AlgorithmFailingNoop) Sum(_ []byte) []byte {
	return nil
}

func (h hashing64AlgorithmFailingNoop) Reset() {}

func (h hashing64AlgorithmFailingNoop) Size() int {
	return 0
}

func (h hashing64AlgorithmFailingNoop) BlockSize() int {
	return 0
}

func (h hashing64AlgorithmFailingNoop) Sum64() uint64 {
	return 0
}

type writerNoopHook struct {
	onWrite      func(context.Context, streams.Message) error
	onWriteBatch func(context.Context, ...streams.Message) (uint32, error)
}

var _ streams.Writer = writerNoopHook{}

func (p writerNoopHook) Write(ctx context.Context, message streams.Message) error {
	if p.onWrite != nil {
		return p.onWrite(ctx, message)
	}
	return nil
}

func (p writerNoopHook) WriteBatch(ctx context.Context, messages ...streams.Message) (uint32, error) {
	if p.onWriteBatch != nil {
		return p.onWriteBatch(ctx, messages...)
	}
	return 0, nil
}

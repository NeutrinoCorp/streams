package streamhub_test

import (
	"context"
	"errors"
	"hash"

	"github.com/neutrinocorp/streamhub"
)

type listenerDriverNoop struct{}

var _ streamhub.ListenerDriver = listenerDriverNoop{}

// ExecuteTask the no-operation implementation of ListenerDriver
func (l listenerDriverNoop) ExecuteTask(_ context.Context, _ *streamhub.ListenerNode) error {
	return nil
}

type listenerDriverNoopGoroutine struct{}

var _ streamhub.ListenerDriver = listenerDriverNoopGoroutine{}

// ExecuteTask the no-operation implementation of ListenerDriver inside a goroutine
func (l listenerDriverNoopGoroutine) ExecuteTask(_ context.Context, _ *streamhub.ListenerNode) error {
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

type publisherNoopHook struct {
	onPublish      func(context.Context, streamhub.Message) error
	onPublishBatch func(context.Context, ...streamhub.Message) error
}

var _ streamhub.Publisher = publisherNoopHook{}

func (p publisherNoopHook) Publish(ctx context.Context, message streamhub.Message) error {
	if p.onPublish != nil {
		return p.onPublish(ctx, message)
	}
	return nil
}

func (p publisherNoopHook) PublishBatch(ctx context.Context, messages ...streamhub.Message) error {
	if p.onPublishBatch != nil {
		return p.onPublishBatch(ctx, messages...)
	}
	return nil
}

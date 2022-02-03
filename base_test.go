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
func (l listenerDriverNoop) ExecuteTask(_ context.Context, _ streamhub.ListenerTask) error {
	return nil
}

type listenerDriverNoopGoroutine struct{}

var _ streamhub.ListenerDriver = listenerDriverNoopGoroutine{}

// ExecuteTask the no-operation implementation of ListenerDriver inside a goroutine
func (l listenerDriverNoopGoroutine) ExecuteTask(_ context.Context, _ streamhub.ListenerTask) error {
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

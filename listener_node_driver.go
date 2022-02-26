package streamhub

import (
	"context"
)

// ListenerDriver defines the underlying implementation of the stream-listening job, which addresses the usage
// of custom protocols and/or APIs from providers (Apache Kafka, Amazon SQS, ...).
type ListenerDriver interface {
	// ExecuteTask addresses the stream-listening task
	ExecuteTask(_ context.Context, _ *ListenerNode) error
}

type listenerDriverNoop struct{}

var _ ListenerDriver = listenerDriverNoop{}

// ExecuteTask the no-operation implementation of ListenerDriver
func (l listenerDriverNoop) ExecuteTask(_ context.Context, _ *ListenerNode) error {
	go func() {}()
	return nil
}

type listenerDriverNoopLoop struct{}

var _ ListenerDriver = listenerDriverNoopLoop{}

// ExecuteTask the no-operation implementation of ListenerDriver with a loop breaking only on given context canceled
// to mimic real-world scenarios
func (l listenerDriverNoopLoop) ExecuteTask(ctx context.Context, _ *ListenerNode) error {
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			default:
			}
		}
	}()
	return nil
}

package streams

import (
	"context"
)

// ReaderHandleFunc is the execution process triggered when a message is received from a stream.
//
// Returns an error to indicate the process has failed so Hub will retry the processing using exponential backoff.
type ReaderHandleFunc func(context.Context, Message) error

// ReaderHandler is a wrapping structure of the ReadFunc handler for complex data processing scenarios.
type ReaderHandler interface {
	// Read starts the execution process triggered when a message is received from a stream.
	//
	// Returns an error to indicate the process has failed so Hub will retry the processing using exponential backoff.
	Read(context.Context, Message) error
}

// ReaderHandlerNoop the no-operation implementation of ReaderHandler
type ReaderHandlerNoop struct{}

var _ ReaderHandler = ReaderHandlerNoop{}

// Read the no-operation implementation of ReaderHandler.Read()
func (l ReaderHandlerNoop) Read(_ context.Context, _ Message) error {
	return nil
}

// Reader defines the underlying implementation of the stream-reading job (driver), which addresses the usage
// of custom protocols and/or APIs from providers (Apache Kafka, Amazon SQS, ...).
type Reader interface {
	// ExecuteTask starts a background stream-reading task.
	ExecuteTask(_ context.Context, _ ReaderTask) error
}

type readerNoop struct{}

var _ Reader = readerNoop{}

// ExecuteTask the no-operation implementation of Reader
func (l readerNoop) ExecuteTask(_ context.Context, _ ReaderTask) error {
	go func() {}()
	return nil
}

type readerNoopLoop struct{}

var _ Reader = readerNoopLoop{}

// ExecuteTask the no-operation implementation of Reader with a loop breaking only on given context canceled
// to mimic real-world scenarios
func (l readerNoopLoop) ExecuteTask(ctx context.Context, _ ReaderTask) error {
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

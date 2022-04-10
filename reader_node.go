package streams

import (
	"context"
	"time"
)

// ReaderNode is the worker unit which schedules stream-reading job(s).
//
// Each ReaderNode is independent of other nodes to guarantee resiliency of interleaved processes and avoid cascading failures.
type ReaderNode struct {
	Stream                string
	HandlerFunc           ReaderHandleFunc
	Group                 string
	ProviderConfiguration interface{}
	ConcurrencyLevel      int
	RetryInitialInterval  time.Duration
	RetryMaxInterval      time.Duration
	RetryTimeout          time.Duration
	Reader                Reader
	MaxHandlerPoolSize    int
}

// start schedules all workers of a ReaderNode.
// Note: Will not schedule if ListenerDriver was not found.
func (n *ReaderNode) start(ctx context.Context) {
	if n.Reader == nil {
		return
	}
	baseTask := newReaderTask(n)
	for i := 0; i < n.ConcurrencyLevel; i++ {
		// TODO: Implement logging to log errors
		_ = n.Reader.ExecuteTask(ctx, baseTask)
	}
	return
}

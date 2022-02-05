package streamhub

import (
	"context"
	"time"
)

// ListenerNode is the worker unit which schedules stream-listening job(s).
//
// Each ListenerNode is independent of other nodes to guarantee resiliency of interleaved processes and avoid cascading failures.
type ListenerNode struct {
	Stream                string
	HandlerFunc           ListenerFunc
	Group                 string
	ProviderConfiguration interface{}
	ConcurrencyLevel      int
	RetryInitialInterval  time.Duration
	RetryMaxInterval      time.Duration
	RetryTimeout          time.Duration
	ListenerDriver        ListenerDriver
}

// start schedules all workers of a ListenerNode.
// Note: Will not schedule if ListenerDriver was not found.
func (n *ListenerNode) start(ctx context.Context) {
	if n.ListenerDriver == nil {
		return
	}
	baseTask := newListenerTask(n)
	for i := 0; i < n.ConcurrencyLevel; i++ {
		// TODO: Implement logging to log errors
		_ = n.ListenerDriver.ExecuteTask(ctx, baseTask)
	}
	return
}

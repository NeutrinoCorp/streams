package streamhub

import (
	"context"
	"time"
)

// listenerNode is the worker unit which schedules stream-listening job(s).
//
// Each ListenerNode is independent of other nodes to guarantee resiliency of interleaved processes and avoid cascading failures.
type listenerNode struct {
	Stream                string
	HandlerFunc           ListenerNodeHandler
	Group                 string
	ProviderConfiguration interface{}
	ConcurrencyLevel      int
	RetryInitialInterval  time.Duration
	RetryMaxInterval      time.Duration
	RetryTimeout          time.Duration
	ListenerDriver        ListenerDriver
}

// start schedules all workers of a ListenerNode.
func (n *listenerNode) start(ctx context.Context) {
	baseTask := newListenerTask(n)
	for i := 0; i < n.ConcurrencyLevel; i++ {
		// TODO: Implement logging to log errors
		_ = n.ListenerDriver.ExecuteTask(ctx, baseTask)
	}
	return
}

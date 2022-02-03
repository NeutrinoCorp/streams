package streamhub

import (
	"context"
	"time"
)

var (
	// DefaultConcurrencyLevel default stream-listening jobs to be running concurrently for each ListenerNode.
	DefaultConcurrencyLevel = 1
	// DefaultMaxRetries default amount of retries for failing stream-listening jobs.
	DefaultMaxRetries = uint32(15)
	// DefaultRetryBackoff default interval duration between each stream-listening job provisioning on failures.
	DefaultRetryBackoff = time.Second * 5
	// DefaultRetryTimeout default duration of each stream-listening job provisioning on failures.
	DefaultRetryTimeout = time.Second * 15
)

type listenerSupervisor struct {
	parentHub            *Hub
	listenerRegistry     listenerRegistry
	baseListenerNodeOpts []ListenerNodeOption
}

func newListenerSupervisor(h *Hub, opts ...ListenerNodeOption) *listenerSupervisor {
	return &listenerSupervisor{
		parentHub:            h,
		listenerRegistry:     make([]listenerNode, 0),
		baseListenerNodeOpts: opts,
	}
}

// forkNode registers a new stream-listening node for later scheduling.
func (s *listenerSupervisor) forkNode(stream string, opts ...ListenerNodeOption) {
	if stream == "" {
		return
	}
	baseOpts := listenerNodeOptions{
		concurrencyLevel: DefaultConcurrencyLevel,
		maxRetries:       DefaultMaxRetries,
		retryBackoff:     DefaultRetryBackoff,
		retryTimeout:     DefaultRetryTimeout,
		driver:           s.parentHub.BaseListenerDriver,
	}
	for _, o := range s.baseListenerNodeOpts {
		o.apply(&baseOpts)
	}
	for _, o := range opts {
		o.apply(&baseOpts)
	}

	s.listenerRegistry = append(s.listenerRegistry, listenerNode{
		Stream:                stream,
		HandlerFunc:           s.newHandlerFuncWrapper(baseOpts),
		Group:                 baseOpts.group,
		ProviderConfiguration: baseOpts.providerConfiguration,
		ConcurrencyLevel:      baseOpts.concurrencyLevel,
		MaxRetries:            baseOpts.maxRetries,
		RetryBackoff:          baseOpts.retryBackoff,
		RetryTimeout:          baseOpts.retryTimeout,
		ListenerDriver:        baseOpts.driver,
	})
}

func (s *listenerSupervisor) newHandlerFuncWrapper(baseOpts listenerNodeOptions) ListenerNodeHandler {
	if baseOpts.listenerFunc == nil && baseOpts.listener == nil {
		return nil
	}
	return func(ctx context.Context, message Message) error {
		// - Retry backoff
		// - Logging
		// - Metrics
		// - Tracing
		// - Correlation and causation ID injection
		message.GroupName = baseOpts.group
		var handler ListenerNodeHandler
		if baseOpts.listener != nil {
			handler = baseOpts.listener.Listen
		} else if baseOpts.listenerFunc != nil {
			handler = ListenerNodeHandler(baseOpts.listenerFunc)
		}

		handler = listenerNodeHandlerRetryBackoff(baseOpts, handler)
		handler = listenerNodeHandlerUnmarshaling(s.parentHub, handler)
		return handler(ctx, message)
	}
}

// startNodes boots up all nodes from the listenerSupervisor's ListenerRegistry.
func (s *listenerSupervisor) startNodes(ctx context.Context) {
	for _, node := range s.listenerRegistry {
		node.start(ctx)
	}
}

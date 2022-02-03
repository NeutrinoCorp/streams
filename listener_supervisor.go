package streamhub

import (
	"context"
	"time"
)

var (
	// DefaultConcurrencyLevel default stream-listening jobs to be running concurrently for each ListenerNode.
	DefaultConcurrencyLevel = 1
	// DefaultRetryInitialInterval default initial interval duration between each stream-listening job provisioning on failures.
	DefaultRetryInitialInterval = time.Second * 3
	// DefaultRetryMaxInterval default maximum interval duration between each stream-listening job provisioning on failures.
	DefaultRetryMaxInterval = time.Second * 15
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
		concurrencyLevel:     DefaultConcurrencyLevel,
		retryInitialInterval: DefaultRetryInitialInterval,
		retryMaxInterval:     DefaultRetryMaxInterval,
		retryTimeout:         DefaultRetryTimeout,
		driver:               s.parentHub.ListenerDriver,
	}
	for _, o := range s.baseListenerNodeOpts {
		o.apply(&baseOpts)
	}
	for _, o := range opts {
		o.apply(&baseOpts)
	}

	s.listenerRegistry = append(s.listenerRegistry, listenerNode{
		Stream:                stream,
		HandlerFunc:           s.attachDefaultBehaviours(baseOpts),
		Group:                 baseOpts.group,
		ProviderConfiguration: baseOpts.providerConfiguration,
		ConcurrencyLevel:      baseOpts.concurrencyLevel,
		RetryInitialInterval:  baseOpts.retryInitialInterval,
		RetryMaxInterval:      baseOpts.retryMaxInterval,
		RetryTimeout:          baseOpts.retryTimeout,
		ListenerDriver:        baseOpts.driver,
	})
}

// Adds to stream-listening handler the following behaviours:
// - Retry backoff
// - Correlation and causation ID injection
// - Unmarshalling*
// - Logging*
// - Metrics*
// - Tracing*
//
// * Optional
func (s *listenerSupervisor) attachDefaultBehaviours(baseOpts listenerNodeOptions) ListenerNodeHandler {
	if baseOpts.listenerFunc == nil && baseOpts.listener == nil {
		return nil
	}

	var handler ListenerNodeHandler
	if baseOpts.listener != nil {
		handler = baseOpts.listener.Listen
	} else if baseOpts.listenerFunc != nil {
		handler = ListenerNodeHandler(baseOpts.listenerFunc)
	}
	handler = retryListenerNodeBehaviour(baseOpts, handler)
	handler = unmarshalListenerNodeBehaviour(s.parentHub, handler)
	handler = injectGroupListenerNodeBehaviour(baseOpts, handler)
	return handler
}

// startNodes boots up all nodes from the listenerSupervisor's ListenerRegistry.
func (s *listenerSupervisor) startNodes(ctx context.Context) {
	for _, node := range s.listenerRegistry {
		node.start(ctx)
	}
}

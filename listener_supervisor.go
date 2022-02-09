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

func newListenerSupervisor(h *Hub) *listenerSupervisor {
	return &listenerSupervisor{
		parentHub:            h,
		listenerRegistry:     make([]ListenerNode, 0),
		baseListenerNodeOpts: h.ListenerBaseOptions,
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

	node := &ListenerNode{
		Stream:                stream,
		HandlerFunc:           s.getFallbackListenerFunc(baseOpts),
		Group:                 baseOpts.group,
		ProviderConfiguration: baseOpts.providerConfiguration,
		ConcurrencyLevel:      baseOpts.concurrencyLevel,
		RetryInitialInterval:  baseOpts.retryInitialInterval,
		RetryMaxInterval:      baseOpts.retryMaxInterval,
		RetryTimeout:          baseOpts.retryTimeout,
		ListenerDriver:        baseOpts.driver,
	}
	node.HandlerFunc = s.attachDefaultBehaviours(node)
	s.listenerRegistry = append(s.listenerRegistry, *node)
}

func (s *listenerSupervisor) getFallbackListenerFunc(baseOpts listenerNodeOptions) ListenerFunc {
	if baseOpts.listenerFunc == nil && baseOpts.listener == nil {
		return nil
	}

	var handler ListenerFunc
	if baseOpts.listener != nil {
		handler = baseOpts.listener.Listen
	} else if baseOpts.listenerFunc != nil {
		handler = baseOpts.listenerFunc
	}
	return handler
}

func (s *listenerSupervisor) attachDefaultBehaviours(node *ListenerNode) ListenerFunc {
	if node.HandlerFunc == nil {
		return nil
	}
	for _, b := range s.parentHub.ListenerBehaviours {
		node.HandlerFunc = b(node, s.parentHub, node.HandlerFunc)
	}
	return node.HandlerFunc
}

// startNodes boots up all nodes from the listenerSupervisor's ListenerRegistry.
func (s *listenerSupervisor) startNodes(ctx context.Context) {
	for _, node := range s.listenerRegistry {
		node.start(ctx)
	}
}

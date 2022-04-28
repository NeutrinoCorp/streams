package streams

import (
	"context"
	"time"

	"github.com/emirpasic/gods/lists/singlylinkedlist"
)

var (
	// DefaultConcurrencyLevel default stream-listening jobs to be running concurrently for each ReaderNode.
	DefaultConcurrencyLevel = 1
	// DefaultRetryInitialInterval default initial interval duration between each stream-listening job provisioning on failures.
	DefaultRetryInitialInterval = time.Second * 3
	// DefaultRetryMaxInterval default maximum interval duration between each stream-listening job provisioning on failures.
	DefaultRetryMaxInterval = time.Second * 15
	// DefaultRetryTimeout default duration of each stream-listening job provisioning on failures.
	DefaultRetryTimeout = time.Second * 15
	// DefaultMaxHandlerPoolSize default pool size of goroutines for ReaderNode's Reader(s) / ReaderHandleFunc(s) executions.
	DefaultMaxHandlerPoolSize = 10
)

type readerSupervisor struct {
	parentHub          *Hub
	readerRegistry     map[string]*singlylinkedlist.List
	baseReaderNodeOpts []ReaderNodeOption
}

func newReaderSupervisor(h *Hub) *readerSupervisor {
	return &readerSupervisor{
		parentHub:          h,
		readerRegistry:     map[string]*singlylinkedlist.List{},
		baseReaderNodeOpts: h.ReaderBaseOptions,
	}
}

// forkNode registers a new stream-listening node for later scheduling.
func (s *readerSupervisor) forkNode(stream string, opts ...ReaderNodeOption) {
	if stream == "" {
		return
	}
	baseOpts := readerNodeOptions{
		concurrencyLevel:     DefaultConcurrencyLevel,
		retryInitialInterval: DefaultRetryInitialInterval,
		retryMaxInterval:     DefaultRetryMaxInterval,
		retryTimeout:         DefaultRetryTimeout,
		driver:               s.parentHub.Reader,
		maxHandlerPoolSize:   DefaultMaxHandlerPoolSize,
	}
	for _, o := range s.baseReaderNodeOpts {
		o.apply(&baseOpts)
	}
	for _, o := range opts {
		o.apply(&baseOpts)
	}

	node := ReaderNode{
		Stream:                stream,
		HandlerFunc:           s.ReaderHandleFunc(baseOpts),
		Group:                 baseOpts.group,
		ProviderConfiguration: baseOpts.providerConfiguration,
		ConcurrencyLevel:      baseOpts.concurrencyLevel,
		RetryInitialInterval:  baseOpts.retryInitialInterval,
		RetryMaxInterval:      baseOpts.retryMaxInterval,
		RetryTimeout:          baseOpts.retryTimeout,
		Reader:                baseOpts.driver,
		MaxHandlerPoolSize:    baseOpts.maxHandlerPoolSize,
	}
	node.HandlerFunc = s.attachDefaultBehaviours(&node)

	list, ok := s.readerRegistry[stream]
	if !ok || list == nil {
		list = singlylinkedlist.New()
	}

	list.Add(node)
	s.readerRegistry[stream] = list
}

func (s *readerSupervisor) ReaderHandleFunc(baseOpts readerNodeOptions) ReaderHandleFunc {
	if baseOpts.readerFunc == nil && baseOpts.readerHandler == nil {
		return nil
	}

	var handler ReaderHandleFunc
	if baseOpts.readerHandler != nil {
		handler = baseOpts.readerHandler.Read
	} else if baseOpts.readerFunc != nil {
		handler = baseOpts.readerFunc
	}
	return handler
}

func (s *readerSupervisor) attachDefaultBehaviours(node *ReaderNode) ReaderHandleFunc {
	if node.HandlerFunc == nil {
		return nil
	}
	for _, b := range s.parentHub.ReaderBehaviours {
		node.HandlerFunc = b(node, s.parentHub, node.HandlerFunc)
	}
	return node.HandlerFunc
}

// startNodes boots up all nodes from the readerSupervisor's ReaderRegistry.
func (s *readerSupervisor) startNodes(ctx context.Context) {
	for _, list := range s.readerRegistry {
		for _, item := range list.Values() {
			readerNode := item.(ReaderNode)
			readerNode.start(ctx)
		}
	}
}

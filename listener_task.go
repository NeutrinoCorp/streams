package streamhub

import "time"

// ListenerTask job metadata in order to be executed by the ListenerNodeDriver.
type ListenerTask struct {
	Stream        string
	HandlerFunc   ListenerNodeHandler
	Group         string
	Configuration interface{}
	Timeout       time.Duration
}

func newListenerTask(n *listenerNode) ListenerTask {
	return ListenerTask{
		Stream:        n.Stream,
		HandlerFunc:   n.HandlerFunc,
		Group:         n.Group,
		Configuration: n.ProviderConfiguration,
		Timeout:       n.RetryTimeout,
	}
}

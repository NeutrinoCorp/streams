package streams

import "time"

// ReaderTask job metadata in order to be executed by the ListenerNodeDriver.
type ReaderTask struct {
	Stream             string
	HandlerFunc        ReaderHandleFunc
	Group              string
	Configuration      interface{}
	Timeout            time.Duration
	MaxHandlerPoolSize int
}

func newReaderTask(n *ReaderNode) ReaderTask {
	return ReaderTask{
		Stream:             n.Stream,
		HandlerFunc:        n.HandlerFunc,
		Group:              n.Group,
		Configuration:      n.ProviderConfiguration,
		Timeout:            n.RetryTimeout,
		MaxHandlerPoolSize: n.MaxHandlerPoolSize,
	}
}

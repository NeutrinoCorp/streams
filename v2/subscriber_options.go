package streams

type SubscriberOption interface {
	apply(task *subscriberTask)
}

type subReaderOpt struct {
	reader Reader
}

var _ SubscriberOption = subReaderOpt{}

func (s subReaderOpt) apply(task *subscriberTask) {
	task.reader = s.reader
}

func WithReader(r Reader) SubscriberOption {
	return subReaderOpt{reader: r}
}

package streams

type writerOption struct {
	writer Writer
}

var _ PublisherOption = writerOption{}

func (o writerOption) applyPublisher(opts *publisherOpts) {
	opts.writer = o.writer
}

type codecOption struct {
	codec Codec
}

var _ PublisherOption = codecOption{}

var _ BusOption = codecOption{}

func (o codecOption) applyPublisher(opts *publisherOpts) {
	opts.codec = o.codec
}

func (o codecOption) applyBus(opts *busOpts) {
	opts.codec = o.codec
}

type idFactoryOption struct {
	idFactory IdentifierFactoryFunc
}

var _ PublisherOption = idFactoryOption{}

var _ BusOption = idFactoryOption{}

func (o idFactoryOption) applyPublisher(opts *publisherOpts) {
	opts.idFactory = o.idFactory
}

func (o idFactoryOption) applyBus(opts *busOpts) {
	opts.idFactory = o.idFactory
}

type streamRegOption struct {
	regFactory *StreamRegistry
}

var _ PublisherOption = streamRegOption{}

func (o streamRegOption) applyPublisher(opts *publisherOpts) {
	opts.reg = o.regFactory
}

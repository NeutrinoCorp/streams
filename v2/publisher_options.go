package streams

type publisherOpts struct {
	writer    Writer
	codec     Codec
	idFactory IdentifierFactoryFunc
	reg       *StreamRegistry
}

type PublisherOption interface {
	apply(*publisherOpts)
}

type writerOption struct {
	writer Writer
}

var _ PublisherOption = writerOption{}

func (o writerOption) apply(opts *publisherOpts) {
	opts.writer = o.writer
}

func WithWriter(w Writer) PublisherOption {
	return writerOption{writer: w}
}

type codecOption struct {
	codec Codec
}

var _ PublisherOption = codecOption{}

func (o codecOption) apply(opts *publisherOpts) {
	opts.codec = o.codec
}

func WithCodec(c Codec) PublisherOption {
	return codecOption{codec: c}
}

type idFactoryOption struct {
	idFactory IdentifierFactoryFunc
}

var _ PublisherOption = idFactoryOption{}

func (o idFactoryOption) apply(opts *publisherOpts) {
	opts.idFactory = o.idFactory
}

func WithIdentifierFactory(f IdentifierFactoryFunc) PublisherOption {
	return idFactoryOption{idFactory: f}
}

type streamRegOption struct {
	regFactory *StreamRegistry
}

var _ PublisherOption = streamRegOption{}

func (o streamRegOption) apply(opts *publisherOpts) {
	opts.reg = o.regFactory
}

func WithRegistry(reg *StreamRegistry) PublisherOption {
	return streamRegOption{regFactory: reg}
}

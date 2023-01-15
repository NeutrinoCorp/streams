package streams

type publisherOpts struct {
	writer    Writer
	codec     Codec
	idFactory IdentifierFactoryFunc
	reg       *StreamRegistry
}

type PublisherOption interface {
	applyPublisher(*publisherOpts)
}

func WithWriter(w Writer) PublisherOption {
	return writerOption{writer: w}
}

func WithPublisherCodec(c Codec) PublisherOption {
	return codecOption{codec: c}
}

func WithPublisherIdentifierFactory(f IdentifierFactoryFunc) PublisherOption {
	return idFactoryOption{idFactory: f}
}

func WithRegistry(reg *StreamRegistry) PublisherOption {
	return streamRegOption{regFactory: reg}
}

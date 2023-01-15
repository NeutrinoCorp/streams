package streams

type busOpts struct {
	publisherOpts
}

type BusOption interface {
	applyBus(opts *busOpts)
}

func WithCodec(c Codec) BusOption {
	return codecOption{codec: c}
}

func WithIdentifierFactory(f IdentifierFactoryFunc) BusOption {
	return idFactoryOption{idFactory: f}
}

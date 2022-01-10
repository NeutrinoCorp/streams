package streamhub

type hubOptions struct {
	instanceName  string
	publisherFunc PublisherFunc
	marshaler     Marshaler
	idFactory     IDFactoryFunc
}

// HubOption enabled interoperability as it sets a specific configuration to a Hub instance.
type HubOption interface {
	apply(*hubOptions)
}

type instanceNameOption struct {
	Instance string
}

func (o instanceNameOption) apply(opts *hubOptions) {
	opts.instanceName = o.Instance
}

// WithInstanceName sets the name of a Hub instance.
func WithInstanceName(n string) HubOption {
	return instanceNameOption{Instance: n}
}

type publisherFuncOption struct {
	PublisherFunc PublisherFunc
}

func (o publisherFuncOption) apply(opts *hubOptions) {
	opts.publisherFunc = o.PublisherFunc
}

// WithPublisherFunc sets the publisher function of a Hub instance.
//
// If both Publisher and PublisherFunc are defined, Publisher will override PublisherFunc.
func WithPublisherFunc(p PublisherFunc) HubOption {
	return publisherFuncOption{PublisherFunc: p}
}

type marshalerOption struct {
	Marshaler Marshaler
}

func (o marshalerOption) apply(opts *hubOptions) {
	opts.marshaler = o.Marshaler
}

// WithMarshaler sets the default marshaler of a Hub instance.
func WithMarshaler(m Marshaler) HubOption {
	return marshalerOption{Marshaler: m}
}

type idFactoryOption struct {
	IDFactory IDFactoryFunc
}

func (o idFactoryOption) apply(opts *hubOptions) {
	opts.idFactory = o.IDFactory
}

// WithIDFactory sets the default unique identifier factory of a Hub instance.
func WithIDFactory(f IDFactoryFunc) HubOption {
	return idFactoryOption{IDFactory: f}
}

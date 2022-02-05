package streamhub

type hubOptions struct {
	instanceName       string
	publisher          Publisher
	publisherFunc      PublisherFunc
	marshaler          Marshaler
	idFactory          IDFactoryFunc
	schemaRegistry     SchemaRegistry
	driver             ListenerDriver
	listenerBehaviours []ListenerBehaviour
}

// HubOption enables configuration of a Hub instance.
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

type publisherOption struct {
	Publisher Publisher
}

func (o publisherOption) apply(opts *hubOptions) {
	opts.publisher = o.Publisher
}

// WithPublisher sets the publisher of a Hub instance.
//
// If both Publisher and PublisherFunc are defined, Publisher will override PublisherFunc.
func WithPublisher(p Publisher) HubOption {
	return publisherOption{Publisher: p}
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

type listenerDriverOption struct {
	Driver ListenerDriver
}

func (o listenerDriverOption) apply(opts *hubOptions) {
	opts.driver = o.Driver
}

// WithListenerDriver sets the default listener driver of a Hub instance.
func WithListenerDriver(d ListenerDriver) HubOption {
	return listenerDriverOption{Driver: d}
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

type schemaRegistryOption struct {
	SchemaRegistry SchemaRegistry
}

func (o schemaRegistryOption) apply(opts *hubOptions) {
	opts.schemaRegistry = o.SchemaRegistry
}

// WithSchemaRegistry sets the schema registry of a Hub instance for stream message schema definitions.
func WithSchemaRegistry(r SchemaRegistry) HubOption {
	return schemaRegistryOption{SchemaRegistry: r}
}

type listenerBehavioursOption struct {
	Behaviours []ListenerBehaviour
}

func (o listenerBehavioursOption) apply(opts *hubOptions) {
	opts.listenerBehaviours = o.Behaviours
}

// WithListenerBehaviours sets a list of ListenerBehaviour of a Hub instance ready to be executed by every stream-listening job's
// ListenerFunc or Listener component.
func WithListenerBehaviours(b ...ListenerBehaviour) HubOption {
	return listenerBehavioursOption{Behaviours: b}
}

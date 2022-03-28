package streamhub

type hubOptions struct {
	instanceName       string
	writer             Writer
	marshaler          Marshaler
	idFactory          IDFactoryFunc
	schemaRegistry     SchemaRegistry
	driver             ListenerDriver
	listenerBehaviours []ListenerBehaviour
	listenerBaseOpts   []ListenerNodeOption
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

type writerOption struct {
	Writer Writer
}

func (o writerOption) apply(opts *hubOptions) {
	opts.writer = o.Writer
}

// WithWriter sets the writer of a Hub instance.
//
// If both Writer and WriterFunc are defined, Writer will override WriterFunc.
func WithWriter(p Writer) HubOption {
	return writerOption{Writer: p}
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

type listenerBaseOptions struct {
	BaseOpts []ListenerNodeOption
}

func (o listenerBaseOptions) apply(opts *hubOptions) {
	opts.listenerBaseOpts = o.BaseOpts
}

// WithListenerBaseOptions sets a list of ListenerNodeOption of a Hub instance used as global options for each listener node
func WithListenerBaseOptions(opts ...ListenerNodeOption) HubOption {
	return listenerBaseOptions{BaseOpts: opts}
}

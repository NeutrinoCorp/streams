package streams

type hubOptions struct {
	instanceName     string
	writer           Writer
	marshaler        Marshaler
	idFactory        IDFactoryFunc
	schemaRegistry   SchemaRegistry
	driver           Reader
	readerBehaviours []ReaderBehaviour
	readerBaseOpts   []ReaderNodeOption
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

type readerOption struct {
	Driver Reader
}

func (o readerOption) apply(opts *hubOptions) {
	opts.driver = o.Driver
}

// WithReader sets the default reader driver of a Hub instance.
func WithReader(d Reader) HubOption {
	return readerOption{Driver: d}
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

type readerBehavioursOption struct {
	Behaviours []ReaderBehaviour
}

func (o readerBehavioursOption) apply(opts *hubOptions) {
	opts.readerBehaviours = o.Behaviours
}

// WithReaderBehaviours sets a list of ReaderBehaviour of a Hub instance ready to be executed by every
// stream-reading job's ReaderFunc or Reader component.
func WithReaderBehaviours(b ...ReaderBehaviour) HubOption {
	return readerBehavioursOption{Behaviours: b}
}

type readerBaseOptions struct {
	BaseOpts []ReaderNodeOption
}

func (o readerBaseOptions) apply(opts *hubOptions) {
	opts.readerBaseOpts = o.BaseOpts
}

// WithReaderBaseOptions sets a list of ReaderNodeOption of a Hub instance used as global options for each reader node.
func WithReaderBaseOptions(opts ...ReaderNodeOption) HubOption {
	return readerBaseOptions{BaseOpts: opts}
}

package streams

type StreamRegistryOption interface {
	apply(opts *StreamMetadata)
}

type schemaURLOption struct {
	schemaURL string
}

var _ StreamRegistryOption = schemaURLOption{}

func (s schemaURLOption) apply(opts *StreamMetadata) {
	opts.SchemaURL = s.schemaURL
}

func WithSchemaURL(url string) StreamRegistryOption {
	return schemaURLOption{
		schemaURL: url,
	}
}

package streamhub

import (
	"context"
)

// DefaultHubInstanceName default instance names for nameless Hub instances
const DefaultHubInstanceName = "com.streamhub"

// Hub is the main component which enables interactions between several systems through the usage of streams.
type Hub struct {
	InstanceName       string
	StreamRegistry     StreamRegistry
	Publisher          Publisher
	PublisherFunc      PublisherFunc
	Marshaler          Marshaler
	IDFactory          IDFactoryFunc
	SchemaRegistry     SchemaRegistry
	BaseListenerDriver ListenerDriver

	listenerSupervisor *listenerSupervisor
}

// NewHub allocates a new Hub
func NewHub(opts ...HubOption) *Hub {
	baseOpts := newHubDefaults()
	for _, o := range opts {
		o.apply(&baseOpts)
	}
	h := &Hub{
		StreamRegistry:     StreamRegistry{},
		InstanceName:       baseOpts.instanceName,
		Marshaler:          baseOpts.marshaler,
		Publisher:          baseOpts.publisher,
		PublisherFunc:      baseOpts.publisherFunc,
		IDFactory:          baseOpts.idFactory,
		SchemaRegistry:     baseOpts.schemaRegistry,
		BaseListenerDriver: baseOpts.driver,
	}
	h.listenerSupervisor = newListenerSupervisor(h)
	return h
}

// defines the fallback options of a Hub instance.
func newHubDefaults() hubOptions {
	return hubOptions{
		instanceName:  DefaultHubInstanceName,
		publisherFunc: NoopPublisherFunc,
		marshaler:     JSONMarshaler{},
		idFactory:     UuidIdFactory,
	}
}

// RegisterStream creates a relation between a stream message type and metadata.
func (h *Hub) RegisterStream(message interface{}, metadata StreamMetadata) {
	h.StreamRegistry.Set(message, metadata)
}

// RegisterStreamByString creates a relation between a string key and metadata.
func (h *Hub) RegisterStreamByString(messageType string, metadata StreamMetadata) {
	h.StreamRegistry.SetByString(messageType, metadata)
}

// Listen registers a new stream-listening background job.
func (h *Hub) Listen(message interface{}, opts ...ListenerNodeOption) error {
	metadata, err := h.StreamRegistry.Get(message)
	if err != nil {
		return err
	}
	h.listenerSupervisor.forkNode(metadata.Stream, opts...)
	return nil
}

// ListenByStreamKey registers a new stream-listening background job using the raw stream identifier (e.g. topic name).
func (h *Hub) ListenByStreamKey(stream string, opts ...ListenerNodeOption) {
	h.listenerSupervisor.forkNode(stream, opts...)
}

// Start initiates all daemons (e.g. stream-listening jobs) processes
func (h *Hub) Start(ctx context.Context) {
	h.listenerSupervisor.startNodes(ctx)
}

// Publish inserts a message into a stream assigned to the message in the StreamRegistry in order to propagate the
// data to a set of subscribed systems for further processing.
//
// Uses given context to inject correlation and causation IDs.
func (h *Hub) Publish(ctx context.Context, message interface{}) error {
	metadata, err := h.StreamRegistry.Get(message)
	if err != nil {
		return err
	}
	return h.publishMessage(ctx, metadata, message)
}

// PublishByMessageKey inserts a message into a stream using the custom message key from StreamRegistry in order to
// propagate the data to a set of subscribed systems for further processing.
//
// Uses given context to inject correlation and causation IDs.
func (h *Hub) PublishByMessageKey(ctx context.Context, messageKey string, message interface{}) error {
	metadata, err := h.StreamRegistry.GetByString(messageKey)
	if err != nil {
		return err
	}
	return h.publishMessage(ctx, metadata, message)
}

// transforms a primitive message into a CloudEvent message ready for transportation. Therefore, executes a
// message publishing job.
func (h *Hub) publishMessage(ctx context.Context, metadata StreamMetadata, message interface{}) error {
	schemaDef := ""
	var err error
	if h.SchemaRegistry != nil {
		schemaDef, err = h.SchemaRegistry.GetSchemaDefinition(metadata.SchemaDefinitionName,
			metadata.SchemaVersion)
		if err != nil {
			return err
		}
	}

	data, err := h.Marshaler.Marshal(schemaDef, message)
	if err != nil {
		return err
	}

	id, err := h.IDFactory()
	if err != nil {
		return err
	}

	return h.PublishRawMessage(ctx, NewMessage(NewMessageArgs{
		SchemaVersion:        metadata.SchemaVersion,
		Data:                 data,
		ID:                   id,
		Source:               h.InstanceName,
		Stream:               metadata.Stream,
		SchemaDefinitionName: metadata.SchemaDefinitionName,
		ContentType:          h.Marshaler.ContentType(),
	}))
}

// PublishRawMessage inserts a raw transport message into a stream in order to propagate the data to a set
// of subscribed systems for further processing.
//
// Uses given context to inject correlation and causation IDs.
func (h *Hub) PublishRawMessage(ctx context.Context, message Message) error {
	message.CorrelationID = InjectMessageCorrelationID(ctx, message.ID)
	message.CausationID = InjectMessageCausationID(ctx, message.ID)
	if h.Publisher != nil {
		return h.Publisher.Publish(ctx, message)
	}
	return h.PublisherFunc(ctx, message)
}

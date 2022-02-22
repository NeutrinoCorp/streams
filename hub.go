package streamhub

import (
	"context"
)

// DefaultHubInstanceName default instance names for nameless Hub instances
var DefaultHubInstanceName = "com.streamhub"

// Hub is the main component which enables interactions between several systems through the usage of streams.
type Hub struct {
	InstanceName        string
	StreamRegistry      StreamRegistry
	Publisher           Publisher
	Marshaler           Marshaler
	IDFactory           IDFactoryFunc
	SchemaRegistry      SchemaRegistry
	ListenerDriver      ListenerDriver
	ListenerBehaviours  []ListenerBehaviour
	ListenerBaseOptions []ListenerNodeOption

	listenerSupervisor *listenerSupervisor
}

// NewHub allocates a new Hub
func NewHub(opts ...HubOption) *Hub {
	baseOpts := newHubDefaults()
	for _, o := range opts {
		o.apply(&baseOpts)
	}
	h := &Hub{
		StreamRegistry:      StreamRegistry{},
		InstanceName:        baseOpts.instanceName,
		Marshaler:           baseOpts.marshaler,
		Publisher:           baseOpts.publisher,
		IDFactory:           baseOpts.idFactory,
		SchemaRegistry:      baseOpts.schemaRegistry,
		ListenerDriver:      baseOpts.driver,
		ListenerBehaviours:  append(listenerBaseBehaviours, baseOpts.listenerBehaviours...),
		ListenerBaseOptions: baseOpts.listenerBaseOpts,
	}
	h.listenerSupervisor = newListenerSupervisor(h)
	return h
}

// defines the fallback options of a Hub instance.
func newHubDefaults() hubOptions {
	return hubOptions{
		instanceName:     DefaultHubInstanceName,
		publisher:        NoopPublisher,
		marshaler:        JSONMarshaler{},
		idFactory:        UuidIdFactory,
		listenerBaseOpts: make([]ListenerNodeOption, 0),
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
	h.ListenByStreamKey(metadata.Stream, opts...)
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

// PublishBatch inserts a set of messages into a stream assigned on the StreamRegistry in order to propagate the
// data to a set of subscribed systems for further processing.
//
// Uses given context to inject correlation and causation IDs.
//
// If an item from the batch fails, other items will fail too
func (h *Hub) PublishBatch(ctx context.Context, messages ...interface{}) error {
	var (
		metadata               StreamMetadata
		transportMessageBuffer = make([]Message, len(messages))
		transportMessage       Message
		err                    error
	)
	for _, msg := range messages {
		metadata, err = h.StreamRegistry.Get(msg)
		if err != nil {
			return err
		}
		transportMessage, err = h.buildTransportMessage(ctx, metadata, msg)
		if err != nil {
			return err
		}
		transportMessageBuffer = append(transportMessageBuffer, transportMessage)
	}

	return h.PublishRawMessageBatch(ctx, transportMessageBuffer...)
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

// PublishByMessageKeyBatchItems items to be published as batch on the Hub.PublishByMessageKeyBatch() function
type PublishByMessageKeyBatchItems map[string]interface{}

// PublishByMessageKeyBatch inserts a set of messages into a stream using the custom message key from StreamRegistry in order to
// propagate the data to a set of subscribed systems for further processing.
//
// Uses given context to inject correlation and causation IDs.
//
// If an item from the batch fails, other items will fail too
func (h *Hub) PublishByMessageKeyBatch(ctx context.Context, items PublishByMessageKeyBatchItems) error {
	var (
		metadata               StreamMetadata
		transportMessageBuffer = make([]Message, len(items))
		transportMessage       Message
		err                    error
	)
	for messageKey, msg := range items {
		metadata, err = h.StreamRegistry.GetByString(messageKey)
		if err != nil {
			return err
		}
		transportMessage, err = h.buildTransportMessage(ctx, metadata, msg)
		if err != nil {
			return err
		}
		transportMessageBuffer = append(transportMessageBuffer, transportMessage)
	}

	return h.PublishRawMessageBatch(ctx, transportMessageBuffer...)
}

// transforms a primitive message into a CloudEvent message ready for transportation. Therefore, executes a
// message publishing job.
func (h *Hub) buildTransportMessage(ctx context.Context, metadata StreamMetadata, message interface{}) (Message, error) {
	schemaDef := ""
	var err error
	if h.SchemaRegistry != nil {
		schemaDef, err = h.SchemaRegistry.GetSchemaDefinition(metadata.SchemaDefinitionName,
			metadata.SchemaVersion)
		if err != nil {
			return Message{}, err
		}
	}

	data, err := h.Marshaler.Marshal(schemaDef, message)
	if err != nil {
		return Message{}, err
	}

	id, err := h.IDFactory()
	if err != nil {
		return Message{}, err
	}

	transportMsg := NewMessage(NewMessageArgs{
		SchemaVersion:        metadata.SchemaVersion,
		Data:                 data,
		ID:                   id,
		Source:               h.InstanceName,
		Stream:               metadata.Stream,
		SchemaDefinitionName: metadata.SchemaDefinitionName,
		ContentType:          h.Marshaler.ContentType(),
	})
	transportMsg.CorrelationID = InjectMessageCorrelationID(ctx, transportMsg.ID)
	transportMsg.CausationID = InjectMessageCausationID(ctx, transportMsg.CorrelationID)

	event, ok := message.(Event)
	if ok {
		transportMsg.Subject = event.Subject()
	}

	return transportMsg, nil
}

// pushes a single message into a stream using cloud events marshaling
func (h *Hub) publishMessage(ctx context.Context, metadata StreamMetadata, message interface{}) error {
	transportMsg, err := h.buildTransportMessage(ctx, metadata, message)
	if err != nil {
		return err
	}
	return h.PublishRawMessage(ctx, transportMsg)
}

// PublishRawMessage inserts a raw transport message into a stream in order to propagate the data to a set
// of subscribed systems for further processing.
//
// Uses given context to inject correlation and causation IDs.
func (h *Hub) PublishRawMessage(ctx context.Context, message Message) error {
	if h.Publisher == nil {
		return ErrMissingPublisherDriver
	}
	return h.Publisher.Publish(ctx, message)
}

// PublishRawMessageBatch inserts a set of raw transport message into a stream in order to propagate the data to a set
// of subscribed systems for further processing.
//
// Uses given context to inject correlation and causation IDs.
//
// The whole batch will be passed to the underlying Publisher driver implementation as every driver has its own way to
// deal with batches
func (h *Hub) PublishRawMessageBatch(ctx context.Context, messages ...Message) error {
	if h.Publisher == nil {
		return ErrMissingPublisherDriver
	}
	return h.Publisher.PublishBatch(ctx, messages...)
}

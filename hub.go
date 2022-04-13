package streams

import (
	"context"
)

var (
	// DefaultHubInstanceName default instance names for nameless Hub instances
	DefaultHubInstanceName = "com.streams"
)

// Hub is the main component which enables interactions between several systems through the usage of streams.
type Hub struct {
	InstanceName      string
	StreamRegistry    StreamRegistry
	Writer            Writer
	Marshaler         Marshaler
	IDFactory         IDFactoryFunc
	SchemaRegistry    SchemaRegistry
	Reader            Reader
	ReaderBehaviours  []ReaderBehaviour
	ReaderBaseOptions []ReaderNodeOption

	readerSupervisor *readerSupervisor
}

// NewHub allocates a new Hub
func NewHub(opts ...HubOption) *Hub {
	baseOpts := newHubDefaults()
	for _, o := range opts {
		o.apply(&baseOpts)
	}
	h := &Hub{
		StreamRegistry:    StreamRegistry{},
		InstanceName:      baseOpts.instanceName,
		Marshaler:         baseOpts.marshaler,
		Writer:            baseOpts.writer,
		IDFactory:         baseOpts.idFactory,
		SchemaRegistry:    baseOpts.schemaRegistry,
		Reader:            baseOpts.driver,
		ReaderBehaviours:  append(ReaderBaseBehaviours, baseOpts.readerBehaviours...),
		ReaderBaseOptions: baseOpts.readerBaseOpts,
	}
	h.readerSupervisor = newReaderSupervisor(h)
	return h
}

// defines the fallback options of a Hub instance.
func newHubDefaults() hubOptions {
	return hubOptions{
		instanceName:   DefaultHubInstanceName,
		writer:         NoopWriter,
		marshaler:      JSONMarshaler{},
		idFactory:      UuidIdFactory,
		readerBaseOpts: make([]ReaderNodeOption, 0),
	}
}

// GetStreamReaderNodes retrieves ReaderNode(s) from a stream.
func (h *Hub) GetStreamReaderNodes(stream string) []ReaderNode {
	return h.readerSupervisor.readerRegistry[stream]
}

// RegisterStream creates a relation between a stream message type and metadata.
//
// If registering a Google's Protocol Buffer message, DO NOT use a pointer as message schema
// to avoid marshaling problems
func (h *Hub) RegisterStream(message interface{}, metadata StreamMetadata) {
	h.StreamRegistry.Set(message, metadata)
}

// RegisterStreamByString creates a relation between a string key and metadata.
func (h *Hub) RegisterStreamByString(messageType string, metadata StreamMetadata) {
	h.StreamRegistry.SetByString(messageType, metadata)
}

// Read registers a new stream-listening background job.
//
// If listening to a Google's Protocol Buffer message, DO NOT use a pointer as message schema
// to avoid marshaling problems
func (h *Hub) Read(message interface{}, opts ...ReaderNodeOption) error {
	metadata, err := h.StreamRegistry.Get(message)
	if err != nil {
		return err
	}
	h.ReadByStreamKey(metadata.Stream, opts...)
	return nil
}

// ReadByStreamKey registers a new stream-listening background job using the raw stream identifier (e.g. topic name).
func (h *Hub) ReadByStreamKey(stream string, opts ...ReaderNodeOption) {
	h.readerSupervisor.forkNode(stream, opts...)
}

// Start initiates all daemons (e.g. stream-listening jobs) processes
func (h *Hub) Start(ctx context.Context) {
	h.readerSupervisor.startNodes(ctx)
}

// Write inserts a message into a stream assigned to the message in the StreamRegistry in order to propagate the
// data to a set of subscribed systems for further processing.
//
// Uses given context to inject correlation and causation IDs.
func (h *Hub) Write(ctx context.Context, message interface{}) error {
	metadata, err := h.StreamRegistry.Get(message)
	if err != nil {
		return err
	}
	return h.writeMessage(ctx, metadata, message)
}

// WriteBatch inserts a set of messages into a stream assigned on the StreamRegistry in order to propagate the
// data to a set of subscribed systems for further processing.
//
// Uses given context to inject correlation and causation IDs.
//
// If an item from the batch fails, other items will fail too
func (h *Hub) WriteBatch(ctx context.Context, messages ...interface{}) (uint32, error) {
	var (
		metadata               StreamMetadata
		transportMessageBuffer = make([]Message, 0, len(messages))
		transportMessage       Message
		err                    error
	)
	for _, msg := range messages {
		metadata, err = h.StreamRegistry.Get(msg)
		if err != nil {
			return 0, err
		}
		transportMessage, err = h.buildTransportMessage(ctx, metadata, msg)
		if err != nil {
			return 0, err
		}
		transportMessageBuffer = append(transportMessageBuffer, transportMessage)
	}

	return h.WriteRawMessageBatch(ctx, transportMessageBuffer...)
}

// WriteByMessageKey inserts a message into a stream using the custom message key from StreamRegistry in order to
// propagate the data to a set of subscribed systems for further processing.
//
// Uses given context to inject correlation and causation IDs.
func (h *Hub) WriteByMessageKey(ctx context.Context, messageKey string, message interface{}) error {
	metadata, err := h.StreamRegistry.GetByString(messageKey)
	if err != nil {
		return err
	}
	return h.writeMessage(ctx, metadata, message)
}

// WriteByMessageKeyBatchItems items to be written as batch on the Hub.WriteByMessageKeyBatch() function
type WriteByMessageKeyBatchItems map[string]interface{}

// WriteByMessageKeyBatch inserts a set of messages into a stream using the custom message key from StreamRegistry in order to
// propagate the data to a set of subscribed systems for further processing.
//
// Uses given context to inject correlation and causation IDs.
//
// If an item from the batch fails, other items will fail too
func (h *Hub) WriteByMessageKeyBatch(ctx context.Context, items WriteByMessageKeyBatchItems) (uint32, error) {
	var (
		metadata               StreamMetadata
		transportMessageBuffer = make([]Message, 0, len(items))
		transportMessage       Message
		err                    error
	)
	for messageKey, msg := range items {
		metadata, err = h.StreamRegistry.GetByString(messageKey)
		if err != nil {
			return 0, err
		}
		transportMessage, err = h.buildTransportMessage(ctx, metadata, msg)
		if err != nil {
			return 0, err
		}
		transportMessageBuffer = append(transportMessageBuffer, transportMessage)
	}

	return h.WriteRawMessageBatch(ctx, transportMessageBuffer...)
}

// transforms a primitive message into a CloudEvent message ready for transportation.
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
		StreamVersion:        metadata.StreamVersion,
		SchemaDefinitionName: metadata.SchemaDefinitionName,
		ContentType:          h.Marshaler.ContentType(),
	})
	transportMsg.CorrelationID = InjectMessageCorrelationID(ctx, transportMsg.ID)
	transportMsg.CausationID = InjectMessageCausationID(ctx, transportMsg.CorrelationID)

	event, ok := message.(Event)
	if ok {
		transportMsg.Subject = event.GetSubject()
	}

	return transportMsg, nil
}

// pushes a single message into a stream using cloud events marshaling
func (h *Hub) writeMessage(ctx context.Context, metadata StreamMetadata, message interface{}) error {
	transportMsg, err := h.buildTransportMessage(ctx, metadata, message)
	if err != nil {
		return err
	}
	return h.WriteRawMessage(ctx, transportMsg)
}

// WriteRawMessage inserts a raw transport message into a stream in order to propagate the data to a set
// of subscribed systems for further processing.
//
// Uses given context to inject correlation and causation IDs.
func (h *Hub) WriteRawMessage(ctx context.Context, message Message) error {
	if h.Writer == nil {
		return ErrMissingWriterDriver
	}
	return h.Writer.Write(ctx, message)
}

// WriteRawMessageBatch inserts a set of raw transport message into a stream in order to propagate the data to a set
// of subscribed systems for further processing.
//
// Uses given context to inject correlation and causation IDs.
//
// The whole batch will be passed to the underlying Writer driver implementation as every driver has its own way to
// deal with batches
func (h *Hub) WriteRawMessageBatch(ctx context.Context, messages ...Message) (uint32, error) {
	if h.Writer == nil {
		return 0, ErrMissingWriterDriver
	}
	return h.Writer.WriteBatch(ctx, messages...)
}

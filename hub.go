package streamhub

import (
	"context"
)

// Hub is the main component which enables interactions between several systems through the usage of streams.
type Hub struct {
	StreamRegistry StreamRegistry
	InstanceName   string
	PublisherFunc  PublisherFunc
	Marshaler      Marshaler
	IDFactory      IDFactoryFunc
}

// NewHub allocates a new Hub
func NewHub(opts ...HubOption) *Hub {
	baseOpts := newHubDefaults()
	for _, o := range opts {
		o.apply(&baseOpts)
	}
	return &Hub{
		StreamRegistry: StreamRegistry{},
		InstanceName:   baseOpts.instanceName,
		Marshaler:      baseOpts.marshaler,
		PublisherFunc:  baseOpts.publisherFunc,
		IDFactory:      baseOpts.idFactory,
	}
}

func newHubDefaults() hubOptions {
	return hubOptions{
		instanceName:  "com.streamhub",
		publisherFunc: NoopPublisher,
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

// Publish inserts a message into a stream assigned to the message in the StreamRegistry in order to propagate the
// data to a set of subscribed systems for further processing.
func (h *Hub) Publish(ctx context.Context, message interface{}) error {
	metadata, err := h.StreamRegistry.Get(message)
	if err != nil {
		return err
	}
	return h.publishMessage(ctx, metadata, message)
}

func (h *Hub) publishMessage(ctx context.Context, metadata StreamMetadata, message interface{}) error {
	data, err := h.Marshaler.Marshal(message)
	if err != nil {
		return err
	}
	id, err := h.IDFactory()
	if err != nil {
		return err
	}

	return h.PublisherFunc(ctx, NewMessage(NewMessageArgs{
		Data:        data,
		ID:          id,
		Source:      h.InstanceName,
		Metadata:    metadata,
		ContentType: h.Marshaler.ContentType(),
	}))
}

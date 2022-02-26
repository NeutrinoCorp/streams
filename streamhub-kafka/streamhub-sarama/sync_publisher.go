package streamhub_sarama

import (
	"context"

	"github.com/Shopify/sarama"
	"github.com/neutrinocorp/streamhub"
)

// SyncPublisher is the blocking-IO implementation for producing messages into an Apache Kafka cluster.
type SyncPublisher struct {
	Producer  sarama.SyncProducer
	Strategy  PublisherStrategy
	Marshaler streamhub.Marshaler
}

var _ streamhub.Publisher = &SyncPublisher{}

// NewSyncPublisher allocates a new SyncPublisher instance ready to be used.
//
// If no PublisherStrategy was found, PublisherRoundRobinStrategy will be used in place.
//
// If marshaler is not nil, SyncPublisher will use `structured mode` marshalling (encoding data straight to
// the Apache Kafka message `value` field).
// Otherwise, by default it will use binary mode (encoding to Apache Kafka message headers).
//
// More information about marshaling here: https://github.com/cloudevents/spec/blob/v1.0.2/cloudevents/bindings/kafka-protocol-binding.md
func NewSyncPublisher(producer sarama.SyncProducer, marshaler streamhub.Marshaler, strategy PublisherStrategy) *SyncPublisher {
	strategy = getDefaultProducerStrategy(strategy)
	return &SyncPublisher{
		Producer:  producer,
		Strategy:  strategy,
		Marshaler: marshaler,
	}
}

// Publish produces a message into an Apache Kafka cluster
func (p *SyncPublisher) Publish(_ context.Context, message streamhub.Message) error {
	transportMsg, err := newProducerMessage(p.Marshaler, p.Strategy, message)
	if err != nil {
		return err
	}
	_, _, err = p.Producer.SendMessage(transportMsg)
	return err
}

// PublishBatch produces a set of messages into an Apache Kafka cluster
func (p *SyncPublisher) PublishBatch(_ context.Context, messages ...streamhub.Message) error {
	transportMsgsBuffer, err := newProducerMessageBatch(p.Marshaler, p.Strategy, messages...)
	if err != nil {
		return err
	}
	return p.Producer.SendMessages(transportMsgsBuffer)
}

package streamhub_sarama

import (
	"context"
	"time"

	"github.com/Shopify/sarama"
	"github.com/neutrinocorp/streamhub"
)

// PublisherStrategy is an option available for streamhub-based systems to produce messages to Apache Kafka using a specific
// way to generate a message key. Thus, systems using streamhub may take advantages of Apache Kafka features
// like log compaction, Exactly-Once guarantees, custom balanced shards/partitions, store messages of a
// transaction within a specific partition, etc.
type PublisherStrategy int

const (
	// PublisherRoundRobinStrategy is the default PublisherStrategy.
	// It will enable a streamhub.Publisher to produce messages to a random Kafka partition using Round Robin
	// balancing.
	//
	// In a real-world scenario, this strategy will most likely erase any possibility to reach an Exactly-Once message
	// delivery as messages will be stored randomly across an Apache Kafka cluster and partitions.
	PublisherRoundRobinStrategy PublisherStrategy = iota
	// PublisherCorrelationIdStrategy enables a streamhub.Publisher to produce messages to a specific partition
	// (chosen by Apache Kafka internals) using the `correlation id` field as message key. Hence, messages with relation
	// through correlation id will most likely be stored within the same Kafka partition, so the ordering-guarantee
	// factor may be increased while using transactions and correlation IDs.
	//
	// In a real-world scenario, a system might treat messages with the same correlation id as transactions.
	// Thus, this strategy will enable systems to consume these transactions with an Exactly-once delivery guarantee as
	// it will store messages (tx's operations) within the same Kafka partition.
	PublisherCorrelationIdStrategy
	// PublisherSubjectStrategy enables a streamhub.Publisher to produce messages to a specific partition
	// (chosen by Apache Kafka internals) using the `subject` field as message key. Hence, messages with the same subject
	// will most likely be stored within the same Kafka partition.
	//
	// In a real-world scenario, a system might choose an aggregate root unique identifier as subject for messages.
	// Using this strategy will increase the ordering-guarantee factor as every operation made to a specific aggregate
	// root will most likely be stored within the same Kafka partition. Moreover, by using the Apache Kafka log compaction
	// feature, a system might keep the latest operation of an aggregate root automatically. This last feature is
	// great when space efficiency, strong-ordering and no replay of messages is required.
	PublisherSubjectStrategy
	// PublisherMessageIdStrategy enables a streamhub.Publisher to produce messages to a specific partition
	// (chosen by Apache Kafka internals) using the `message id` field as message key. This strategy basically enables
	// the same features as the PublisherRoundRobinStrategy as message id are generated randomly.
	PublisherMessageIdStrategy
)

// newKKey generates a Kafka message key based on the given PublisherStrategy
func newKKey(s PublisherStrategy, msg streamhub.Message) sarama.Encoder {
	switch s {
	case PublisherCorrelationIdStrategy:
		return sarama.StringEncoder(msg.CorrelationID)
	case PublisherSubjectStrategy:
		return sarama.StringEncoder(msg.Subject)
	case PublisherMessageIdStrategy:
		return sarama.StringEncoder(msg.ID)
	default:
		return nil
	}
}

// populateProducerMessage constructs a Kafka message based on either the structured or binary content modes.
//
// It will choose structured mode if a marshaler is not nil. If not, binary mode will be used.
//
// Moreover, binary mode means data will be stored within Kafka message's headers in binary format while structured mode
// means a whole streamhub.Message will be encoded using the given marshaler, so it will be stored within the Kafka message
// value field.
//
// more info here: https://github.com/cloudevents/spec/blob/v1.0.2/cloudevents/bindings/kafka-protocol-binding.md
func populateProducerMessage(marshaler streamhub.Marshaler, message streamhub.Message,
	producerMsgRef *sarama.ProducerMessage) error {
	if marshaler != nil {
		// structured content mode from CloudEvents
		// more info here: https://github.com/cloudevents/spec/blob/v1.0.2/cloudevents/bindings/kafka-protocol-binding.md
		msgEncoded, err := marshaler.Marshal("", message)
		if err != nil {
			return err
		}
		producerMsgRef.Headers = []sarama.RecordHeader{
			{
				Key:   []byte("content-type"),
				Value: []byte(marshaler.ContentType()),
			},
		}
		producerMsgRef.Value = sarama.ByteEncoder(msgEncoded)
		return nil
	}

	// binary content mode from CloudEvents
	producerMsgRef.Headers = serializeKHeaders(message)
	producerMsgRef.Value = sarama.ByteEncoder(message.Data)
	return nil
}

// returns PublisherRoundRobinStrategy if an arbitrary value was passed
func getDefaultProducerStrategy(s PublisherStrategy) PublisherStrategy {
	if s < 0 || s > 4 {
		s = PublisherRoundRobinStrategy
	}
	return s
}

// newProducerMessage generates a sarama.ProducerMessage using the given strategy and, if applies, the given marshaler
// to encode the streamhub.Message into the Kafka Value field
func newProducerMessage(marshaler streamhub.Marshaler, strategy PublisherStrategy,
	message streamhub.Message) (*sarama.ProducerMessage, error) {
	ts, _ := time.Parse(time.RFC3339, message.Timestamp)
	producerMsg := &sarama.ProducerMessage{
		Topic:     message.Stream,
		Key:       newKKey(strategy, message),
		Timestamp: ts,
	}

	if err := populateProducerMessage(marshaler, message, producerMsg); err != nil {
		return nil, err
	}
	return producerMsg, nil
}

// SyncPublisher is the blocking-IO implementation for producing messages into an Apache Kafka cluster.
type SyncPublisher struct {
	Producer  sarama.SyncProducer
	Strategy  PublisherStrategy
	Marshaler streamhub.Marshaler
}

var _ streamhub.Publisher = &SyncPublisher{}

// NewSyncPublisher allocates a new SyncPublisher instance ready to be used.
//
// If no PublisherStrategy was found, PublisherRoundRobinStrategy will be used in place
func NewSyncPublisher(producer sarama.SyncProducer, marshaler streamhub.Marshaler, strategy PublisherStrategy) *SyncPublisher {
	strategy = getDefaultProducerStrategy(strategy)
	return &SyncPublisher{
		Producer:  producer,
		Strategy:  strategy,
		Marshaler: marshaler,
	}
}

// Publish produces a message into an Apache Kafka cluster.
func (p *SyncPublisher) Publish(_ context.Context, message streamhub.Message) error {
	pMessage, err := newProducerMessage(p.Marshaler, p.Strategy, message)
	if err != nil {
		return err
	}
	_, _, err = p.Producer.SendMessage(pMessage)
	return err
}

func (p *SyncPublisher) PublishBatch(ctx context.Context, messages ...streamhub.Message) error {
	panic("implement me")
}

// PublisherErrorHook is a function which will be triggered after an error is detected when producing a message
// to an Apache Kafka cluster.
//
// Only works with AsyncPublisher at the moment
type PublisherErrorHook func(*sarama.ProducerError)

// AsyncPublisher is the non blocking-IO message producer. Useful in high-performance and high-concurrence scenarios as
// it relies on Go's channels to produce messages to an Apache Kafka cluster
type AsyncPublisher struct {
	Producer  sarama.AsyncProducer
	Strategy  PublisherStrategy
	Marshaler streamhub.Marshaler
}

var _ streamhub.Publisher = &AsyncPublisher{}

// NewAsyncPublisher allocates a new AsyncPublisher instance ready to be used.
//
// If no PublisherStrategy was found, PublisherRoundRobinStrategy will be used in place
func NewAsyncPublisher(producer sarama.AsyncProducer, marshaler streamhub.Marshaler, errorHook PublisherErrorHook,
	strategy PublisherStrategy) *AsyncPublisher {
	strategy = getDefaultProducerStrategy(strategy)
	if errorHook != nil {
		go func() {
			for err := range producer.Errors() {
				errorHook(err)
			}
		}()
	}
	return &AsyncPublisher{
		Producer:  producer,
		Strategy:  strategy,
		Marshaler: marshaler,
	}
}

// Publish produces a message to an Apache Kafka cluster without blocking the IO.
//
// The underlying publishing module WILL NOT return an error as the operation will be executed asynchronously
func (p *AsyncPublisher) Publish(_ context.Context, message streamhub.Message) error {
	pMessage, err := newProducerMessage(p.Marshaler, p.Strategy, message)
	if err != nil {
		return err
	}
	p.Producer.Input() <- pMessage
	return nil
}

func (p *AsyncPublisher) PublishBatch(ctx context.Context, messages ...streamhub.Message) error {
	panic("implement me")
}

package streamhub_sarama

import (
	"context"

	"github.com/Shopify/sarama"
	"github.com/neutrinocorp/streamhub"
)

type defaultConsumerGroupHandler struct {
	marshaler streamhub.Marshaler
	node      *streamhub.ListenerNode
	producer  sarama.SyncProducer
}

var _ sarama.ConsumerGroupHandler = defaultConsumerGroupHandler{}

func (d defaultConsumerGroupHandler) Setup(_ sarama.ConsumerGroupSession) error {
	return nil
}

func (d defaultConsumerGroupHandler) Cleanup(_ sarama.ConsumerGroupSession) error {
	return nil
}

func (d defaultConsumerGroupHandler) getReprocessTopic(err error) string {
	if err == nil {
		return ""
	} else if err == errNonRetryableProcess || d.node.RetryStream == "" {
		return d.node.DeadLetterStream
	}
	return d.node.RetryStream
}

func (d defaultConsumerGroupHandler) execReliableReprocessing(err error, kMessage *sarama.ConsumerMessage) error {
	fallbackTopic := d.getReprocessTopic(err)
	if fallbackTopic == "" {
		return nil // invalid topic or no error returned from original process, skip process
	}

	prodMsg := consumerToProducerMessage(kMessage)
	prodMsg.Topic = fallbackTopic
	_, _, err = d.producer.SendMessage(prodMsg)
	return err
}

func (d defaultConsumerGroupHandler) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	// NOTE: DO NOT return any errors if the session is required to be persisted. Returning any error from here will
	// cause the whole consumer group session to fall.

	for kMessage := range claim.Messages() {
		err := processMessage(session.Context(), d.marshaler, d.node, kMessage)
		// if no producer was found, reliable processing is disabled. Fall back to default
		// behaviour (commit on no-error from internal processes).
		//
		// If producer is set, reliable processing is enabled. Furthermore, every message received
		// successfully should be committed to Apache Kafka.
		if err == nil && d.producer == nil {
			session.MarkMessage(kMessage, "")
			session.Commit()
			return nil
		}
		err = d.execReliableReprocessing(err, kMessage)
		if err == nil {
			session.MarkMessage(kMessage, "")
			session.Commit()
		}
	}
	return nil
}

// GroupListenerDriver is the streamhub.ListenerDriver implementation for Apache Kafka consumers components
// who require fetching messages from a topic using Kafka's `Consumer Group` feature.
//
// Useful when message consumption is done from within a cluster (i.e. microservice) in order to reduce message
// re-processing scenarios between workers.
//
// Furthermore, GroupListenerDriver(s) MAY enable `Reliable Reprocessing` feature if desired.
// Reliable Reprocessing is a technique implemented by streamhub (and top-tier companies like Uber) to handle
// successfully message consumption failing scenarios. More information about this technique here:
// https://eng.uber.com/reliable-reprocessing/
type GroupListenerDriver struct {
	producer  sarama.SyncProducer
	marshaler streamhub.Marshaler
}

var _ streamhub.ListenerDriver = GroupListenerDriver{}

// NewGroupListenerDriver allocates a new GroupListenerDriver ready to be used.
//
// If producer is not nil, the GroupListenerDriver will attempt to execute self-healing operations described by
// Uber Engineering in the following blog: https://eng.uber.com/reliable-reprocessing/
//
// If marshaler is not nil, the GroupListenerDriver will use `structured mode` unmarshalling (decoding data straight from
// Apache Kafka message `value` field).
// Otherwise, by default it will use binary mode (decoding data from Apache Kafka message headers).
//
// More information about marshaling here: https://github.com/cloudevents/spec/blob/v1.0.2/cloudevents/bindings/kafka-protocol-binding.md
func NewGroupListenerDriver(producer sarama.SyncProducer,
	marshaler streamhub.Marshaler) GroupListenerDriver {
	return GroupListenerDriver{
		producer:  producer,
		marshaler: marshaler,
	}
}

// ExecuteTask starts the underlying consumer group message consumption
func (g GroupListenerDriver) ExecuteTask(ctx context.Context, node *streamhub.ListenerNode) error {
	cfg, ok := node.ProviderConfiguration.(*sarama.Config)
	if !ok {
		return streamhub.ErrInvalidProviderConfiguration
	}
	consumer, err := sarama.NewConsumerGroup(node.Hosts, node.Group, cfg)
	if err != nil {
		return err
	}
	go func() {
		for {
			err = consumer.Consume(ctx, []string{node.Stream}, &defaultConsumerGroupHandler{
				marshaler: g.marshaler,
				node:      node,
				producer:  g.producer,
			})
			if err != nil && err.Error() != "kafka: tried to use a consumer group that was closed" {
				return
			}
			select {
			case <-ctx.Done():
				_ = consumer.Close()
				return
			default:
			}
		}
	}()

	return nil
}

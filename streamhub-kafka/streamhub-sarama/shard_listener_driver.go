package streamhub_sarama

import (
	"context"

	"github.com/Shopify/sarama"
	"github.com/neutrinocorp/streamhub"
)

// ShardListenerConfiguration defines the message consuming strategy for a (or a set of) ShardListenerDriver(s).
//
// Streamhub-based systems MIGHT set a global configuration through streamhub.ListenerNodeOption and streamhub.Hub
type ShardListenerConfiguration struct {
	Partition int32
	Offset    int64
}

// ShardListenerDriver is the streamhub.ListenerDriver implementation for Apache Kafka consumers components
// who require fetching messages from a specific partition and/or offset
type ShardListenerDriver struct {
	consumer  sarama.Consumer
	marshaler streamhub.Marshaler
}

var _ streamhub.ListenerDriver = ShardListenerDriver{}

// NewShardListenerDriver allocates a new ShardListenerDriver ready to be used
func NewShardListenerDriver(consumer sarama.Consumer, marshaler streamhub.Marshaler) ShardListenerDriver {
	return ShardListenerDriver{
		consumer:  consumer,
		marshaler: marshaler,
	}
}

// ExecuteTask starts consuming messages from the given topic, partition and offset.
//
// If no ShardListenerConfiguration is specified, this operation will return an error before consuming any messages
func (s ShardListenerDriver) ExecuteTask(ctx context.Context, node *streamhub.ListenerNode) error {
	cfg, ok := node.ProviderConfiguration.(ShardListenerConfiguration)
	if !ok {
		return streamhub.ErrInvalidProviderConfiguration
	}

	partitionConsumer, err := s.consumer.ConsumePartition(node.Stream, cfg.Partition, cfg.Offset)
	if err != nil {
		return err
	}

	go func() {
		defer partitionConsumer.AsyncClose()
		for msg := range partitionConsumer.Messages() {
			go func(message *sarama.ConsumerMessage) {
				_ = processMessage(ctx, s.marshaler, node, message)
			}(msg)
		}
	}()
	return nil
}

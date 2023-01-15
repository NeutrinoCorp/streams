package kafka

import (
	"github.com/Shopify/sarama"
	"github.com/modern-go/reflect2"
	"github.com/neutrinocorp/streams/v2"
)

const (
	HeaderKey                         = "streams-kafka-key"
	HeaderOffset                      = "streams-kafka-offset"
	HeaderPartition                   = "streams-kafka-partition"
	HeaderConsumerGroupName           = "streams-kafka-consumer-group-name"
	HeaderConsumerMemberID            = "streams-kafka-consumer-member-id"
	HeaderConsumerGenerationID        = "streams-kafka-consumer-generation-id"
	HeaderConsumerInitialOffset       = "streams-kafka-consumer-initial-offset"
	HeaderConsumerHighWatermarkOffset = "streams-kafka-consumer-high-watermark-offset"
)

var kafkaHeadersSet = map[string]struct{}{
	HeaderKey:                         {},
	HeaderOffset:                      {},
	HeaderPartition:                   {},
	HeaderConsumerGroupName:           {},
	HeaderConsumerMemberID:            {},
	HeaderConsumerGenerationID:        {},
	HeaderConsumerInitialOffset:       {},
	HeaderConsumerHighWatermarkOffset: {},
}

func newKHeaders(msg streams.Message) []sarama.RecordHeader {
	return []sarama.RecordHeader{
		{
			Key:   reflect2.UnsafeCastString(streams.HeaderMessageID),
			Value: reflect2.UnsafeCastString(msg.ID),
		},
		{
			Key:   reflect2.UnsafeCastString(streams.HeaderCorrelationID),
			Value: reflect2.UnsafeCastString(msg.CorrelationID),
		},
		{
			Key:   reflect2.UnsafeCastString(streams.HeaderCausationID),
			Value: reflect2.UnsafeCastString(msg.CausationID),
		},
		{
			Key:   reflect2.UnsafeCastString(streams.HeaderStreamName),
			Value: reflect2.UnsafeCastString(msg.StreamName),
		},
		{
			Key:   reflect2.UnsafeCastString(streams.HeaderContentType),
			Value: reflect2.UnsafeCastString(msg.ContentType),
		},
		{
			Key:   reflect2.UnsafeCastString(streams.HeaderSchemaURL),
			Value: reflect2.UnsafeCastString(msg.SchemaURL),
		},
	}
}

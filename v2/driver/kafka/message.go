package kafka

import (
	"strconv"
	"time"

	"github.com/Shopify/sarama"
	"github.com/modern-go/reflect2"
	"github.com/neutrinocorp/streams/v2"
)

func newKMessage(msg streams.Message) sarama.ProducerMessage {
	key := msg.ID
	if k, ok := msg.Headers[HeaderKey]; ok {
		key = k
	}

	headers := newKHeaders(msg)
	for k, v := range msg.Headers {
		if _, ok := kafkaHeadersSet[k]; ok {
			continue
		}
		headers = append(headers, sarama.RecordHeader{
			Key:   reflect2.UnsafeCastString(k),
			Value: reflect2.UnsafeCastString(v),
		})
	}

	offset, _ := strconv.ParseInt(msg.Headers[HeaderOffset], 10, 64)
	partition, _ := strconv.ParseInt(msg.Headers[HeaderPartition], 10, 32)
	return sarama.ProducerMessage{
		Topic:     msg.StreamName,
		Key:       sarama.StringEncoder(key),
		Value:     sarama.ByteEncoder(msg.Data),
		Headers:   headers,
		Offset:    offset,
		Partition: int32(partition),
		Timestamp: time.UnixMilli(msg.TimestampMillis),
	}
}

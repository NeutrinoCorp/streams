package streams

import (
	"context"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Shopify/sarama"
	"github.com/modern-go/reflect2"
)

const (
	HeaderKafkaKey                         = "streams-kafka-key"
	HeaderKafkaOffset                      = "streams-kafka-offset"
	HeaderKafkaPartition                   = "streams-kafka-partition"
	HeaderKafkaConsumerGroupName           = "streams-kafka-consumer-group-name"
	HeaderKafkaConsumerMemberID            = "streams-kafka-consumer-member-id"
	HeaderKafkaConsumerGenerationID        = "streams-kafka-consumer-generation-id"
	HeaderKafkaConsumerInitialOffset       = "streams-kafka-consumer-initial-offset"
	HeaderKafkaConsumerHighWatermarkOffset = "streams-kafka-consumer-high-watermark-offset"
)

var kafkaHeadersSet = map[string]struct{}{
	HeaderKafkaKey:                         {},
	HeaderKafkaOffset:                      {},
	HeaderKafkaPartition:                   {},
	HeaderKafkaConsumerGroupName:           {},
	HeaderKafkaConsumerMemberID:            {},
	HeaderKafkaConsumerGenerationID:        {},
	HeaderKafkaConsumerInitialOffset:       {},
	HeaderKafkaConsumerHighWatermarkOffset: {},
}

func newKafkaHeaders(msg Message) []sarama.RecordHeader {
	return []sarama.RecordHeader{
		{
			Key:   reflect2.UnsafeCastString(HeaderMessageID),
			Value: reflect2.UnsafeCastString(msg.ID),
		},
		{
			Key:   reflect2.UnsafeCastString(HeaderCorrelationID),
			Value: reflect2.UnsafeCastString(msg.CorrelationID),
		},
		{
			Key:   reflect2.UnsafeCastString(HeaderCausationID),
			Value: reflect2.UnsafeCastString(msg.CausationID),
		},
		{
			Key:   reflect2.UnsafeCastString(HeaderStreamName),
			Value: reflect2.UnsafeCastString(msg.StreamName),
		},
		{
			Key:   reflect2.UnsafeCastString(HeaderContentType),
			Value: reflect2.UnsafeCastString(msg.ContentType),
		},
		{
			Key:   reflect2.UnsafeCastString(HeaderSchemaURL),
			Value: reflect2.UnsafeCastString(msg.SchemaURL),
		},
	}
}

func newKafkaMessage(msg Message) sarama.ProducerMessage {
	key := msg.ID
	if k, ok := msg.Headers[HeaderKafkaKey]; ok {
		key = k
	}

	headers := newKafkaHeaders(msg)
	for k, v := range msg.Headers {
		if _, ok := kafkaHeadersSet[k]; ok {
			continue
		}
		headers = append(headers, sarama.RecordHeader{
			Key:   reflect2.UnsafeCastString(k),
			Value: reflect2.UnsafeCastString(v),
		})
	}

	offset, _ := strconv.ParseInt(msg.Headers[HeaderKafkaOffset], 10, 64)
	partition, _ := strconv.ParseInt(msg.Headers[HeaderKafkaPartition], 10, 32)
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

type KafkaSyncWriter struct {
	client sarama.SyncProducer
}

var _ Writer = KafkaSyncWriter{}

func NewKafkaSyncWriter(c sarama.SyncProducer) KafkaSyncWriter {
	return KafkaSyncWriter{client: c}
}

func (k KafkaSyncWriter) Write(ctx context.Context, p Message) (int, error) {
	return k.WriteMany(ctx, []Message{p})
}

func (k KafkaSyncWriter) WriteMany(_ context.Context, p []Message) (n int, err error) {
	msgs := make([]*sarama.ProducerMessage, 0, len(p))
	for _, msg := range p {
		kMsg := newKafkaMessage(msg)
		msgs = append(msgs, &kMsg)
		n += kMsg.Value.Length()
	}
	err = k.client.SendMessages(msgs)
	return
}

func (k KafkaSyncWriter) Close() error {
	return k.client.Close()
}

type KafkaAsyncWriter struct {
	client sarama.AsyncProducer
}

var _ Writer = KafkaAsyncWriter{}

func NewKafkaAsyncWriter(p sarama.AsyncProducer) KafkaAsyncWriter {
	return KafkaAsyncWriter{
		client: p,
	}
}

func (k KafkaAsyncWriter) Write(ctx context.Context, p Message) (n int, err error) {
	return k.WriteMany(ctx, []Message{p})
}

func (k KafkaAsyncWriter) WriteMany(_ context.Context, p []Message) (n int, err error) {
	wg := sync.WaitGroup{}
	wg.Add(len(p))
	var bytesOut uintptr = 0
	for _, msg := range p {
		kMsg := newKafkaMessage(msg)
		k.client.Input() <- &kMsg
	}

	go func() {
		for err = range k.client.Errors() {
			wg.Done()
		}
	}()
	go func() {
		for m := range k.client.Successes() {
			atomic.AddUintptr(&bytesOut, uintptr(m.Value.Length()))
			wg.Done()
		}
	}()
	wg.Wait()
	n = int(bytesOut)
	return
}

func (k KafkaAsyncWriter) Close() error {
	return k.client.Close()
}

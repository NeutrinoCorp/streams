package kafka

import (
	"strconv"
	"time"

	"github.com/Shopify/sarama"
	"github.com/modern-go/reflect2"
	"github.com/neutrinocorp/streams/v2"
)

func newKProducerMessage(msg streams.Message) sarama.ProducerMessage {
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

type newKConsumerMessageArgs struct {
	Key                 []byte
	Offset              int64
	InitialOffset       int64
	HighWaterMarkOffset int64
	Partition           int32
	GenerationID        int32
	MemberID            string
	GroupName           string
	Headers             []*sarama.RecordHeader
	Value               []byte
	Timestamp           time.Time
}

func newKConsumerMessage(args newKConsumerMessageArgs) streams.Message {
	msgMetadataMap := map[string]string{}
	headers := map[string]string{
		HeaderKey:                         string(args.Key),
		HeaderOffset:                      strconv.FormatInt(args.Offset, 10),
		HeaderPartition:                   strconv.FormatInt(int64(args.Partition), 10),
		HeaderConsumerGroupName:           args.GroupName,
		HeaderConsumerMemberID:            args.MemberID,
		HeaderConsumerGenerationID:        strconv.FormatInt(int64(args.GenerationID), 10),
		HeaderConsumerInitialOffset:       strconv.FormatInt(args.InitialOffset, 10),
		HeaderConsumerHighWatermarkOffset: strconv.FormatInt(args.HighWaterMarkOffset, 10),
	}
	for _, h := range args.Headers {
		key := string(h.Key)
		val := string(h.Value)
		if _, ok := streams.HeaderSet[key]; ok {
			msgMetadataMap[key] = val
			continue
		}
		headers[key] = val
	}

	return streams.Message{
		ID:              msgMetadataMap[streams.HeaderMessageID],
		CorrelationID:   msgMetadataMap[streams.HeaderCorrelationID],
		CausationID:     msgMetadataMap[streams.HeaderCausationID],
		StreamName:      msgMetadataMap[streams.HeaderStreamName],
		ContentType:     msgMetadataMap[streams.HeaderContentType],
		SchemaURL:       msgMetadataMap[streams.HeaderSchemaURL],
		Data:            args.Value,
		TimestampMillis: args.Timestamp.UnixMilli(),
		Headers:         headers,
	}
}

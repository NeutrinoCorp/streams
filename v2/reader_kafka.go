package streams

import (
	"context"
	"log"
	"strconv"
	"sync"

	"github.com/Shopify/sarama"
)

type kafkaConsumerGroupHandler struct {
	groupName string
	subFunc   SubscriberFunc
}

var _ sarama.ConsumerGroupHandler = kafkaConsumerGroupHandler{}

func (k kafkaConsumerGroupHandler) Setup(_ sarama.ConsumerGroupSession) error {
	log.Print("setting up consumer handler")
	return nil
}

func (k kafkaConsumerGroupHandler) Cleanup(_ sarama.ConsumerGroupSession) error {
	log.Print("cleaning up consumer handler")
	return nil
}

func (k kafkaConsumerGroupHandler) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for msg := range claim.Messages() {
		msgMetadataMap := map[string]string{}
		headers := map[string]string{
			HeaderKafkaKey:                         string(msg.Key),
			HeaderKafkaOffset:                      strconv.FormatInt(msg.Offset, 10),
			HeaderKafkaPartition:                   strconv.FormatInt(int64(msg.Partition), 10),
			HeaderKafkaConsumerGroupName:           k.groupName,
			HeaderKafkaConsumerMemberID:            session.MemberID(),
			HeaderKafkaConsumerGenerationID:        strconv.FormatInt(int64(session.GenerationID()), 10),
			HeaderKafkaConsumerInitialOffset:       strconv.FormatInt(claim.InitialOffset(), 10),
			HeaderKafkaConsumerHighWatermarkOffset: strconv.FormatInt(claim.HighWaterMarkOffset(), 10),
		}
		for _, h := range msg.Headers {
			key := string(h.Key)
			val := string(h.Value)
			if _, ok := HeaderSet[key]; ok {
				msgMetadataMap[key] = val
				continue
			}
			headers[key] = val
		}

		scopedCtx, cancel := context.WithCancel(session.Context())
		if err := k.subFunc(scopedCtx, Message{
			ID:              msgMetadataMap[HeaderMessageID],
			CorrelationID:   msgMetadataMap[HeaderCorrelationID],
			CausationID:     msgMetadataMap[HeaderCausationID],
			StreamName:      msgMetadataMap[HeaderStreamName],
			ContentType:     msgMetadataMap[HeaderContentType],
			SchemaURL:       msgMetadataMap[HeaderSchemaURL],
			Data:            msg.Value,
			TimestampMillis: msg.Timestamp.UnixMilli(),
			Headers:         headers,
		}); err != nil {
			cancel()
			continue
		}
		session.MarkMessage(msg, "")
		cancel()
	}
	return nil
}

type KafkaReader struct {
	groupID     string
	client      sarama.ConsumerGroup
	handlerPool *sync.Pool
}

func NewKafkaReader(groupID string, c sarama.ConsumerGroup) KafkaReader {
	return KafkaReader{
		client: c,
		handlerPool: &sync.Pool{
			New: func() interface{} {
				return kafkaConsumerGroupHandler{
					groupName: groupID,
				}
			},
		},
	}
}

func (r KafkaReader) Read(ctx context.Context, stream string, subscriberFunc SubscriberFunc) (err error) {
	handler := r.handlerPool.Get().(kafkaConsumerGroupHandler)
	defer func() {
		r.handlerPool.Put(handler)
	}()
	handler.subFunc = subscriberFunc
	go func() {
		for {
			if errConsumer := r.client.Consume(ctx, []string{stream}, handler); errConsumer != nil &&
				errConsumer != sarama.ErrClosedConsumerGroup {
				err = errConsumer
				break
			}
			select {
			case <-r.client.Errors():
				continue
			}
		}
	}()

	select {
	case <-ctx.Done():
		log.Print("closing reader")
		return
	}
}

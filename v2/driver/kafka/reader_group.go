package kafka

import (
	"context"
	"log"
	"strconv"
	"sync"

	"github.com/Shopify/sarama"
	"github.com/neutrinocorp/streams/v2"
)

type kafkaConsumerGroupHandler struct {
	groupName string
	subFunc   streams.SubscriberFunc
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
			HeaderKey:                         string(msg.Key),
			HeaderOffset:                      strconv.FormatInt(msg.Offset, 10),
			HeaderPartition:                   strconv.FormatInt(int64(msg.Partition), 10),
			HeaderConsumerGroupName:           k.groupName,
			HeaderConsumerMemberID:            session.MemberID(),
			HeaderConsumerGenerationID:        strconv.FormatInt(int64(session.GenerationID()), 10),
			HeaderConsumerInitialOffset:       strconv.FormatInt(claim.InitialOffset(), 10),
			HeaderConsumerHighWatermarkOffset: strconv.FormatInt(claim.HighWaterMarkOffset(), 10),
		}
		for _, h := range msg.Headers {
			key := string(h.Key)
			val := string(h.Value)
			if _, ok := streams.HeaderSet[key]; ok {
				msgMetadataMap[key] = val
				continue
			}
			headers[key] = val
		}

		scopedCtx, cancel := context.WithCancel(session.Context())
		if err := k.subFunc(scopedCtx, streams.Message{
			ID:              msgMetadataMap[streams.HeaderMessageID],
			CorrelationID:   msgMetadataMap[streams.HeaderCorrelationID],
			CausationID:     msgMetadataMap[streams.HeaderCausationID],
			StreamName:      msgMetadataMap[streams.HeaderStreamName],
			ContentType:     msgMetadataMap[streams.HeaderContentType],
			SchemaURL:       msgMetadataMap[streams.HeaderSchemaURL],
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

type ReaderGroup struct {
	groupID     string
	client      sarama.ConsumerGroup
	handlerPool *sync.Pool
}

func NewReaderGroup(groupID string, c sarama.ConsumerGroup) ReaderGroup {
	return ReaderGroup{
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

func (r ReaderGroup) Read(ctx context.Context, stream string, subscriberFunc streams.SubscriberFunc) (err error) {
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

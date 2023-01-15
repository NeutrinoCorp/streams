package kafka

import (
	"context"
	"log"
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
		streamMsg := newKConsumerMessage(newKConsumerMessageArgs{
			Key:                 msg.Key,
			Offset:              msg.Offset,
			InitialOffset:       claim.InitialOffset(),
			HighWaterMarkOffset: claim.HighWaterMarkOffset(),
			Partition:           msg.Partition,
			GenerationID:        session.GenerationID(),
			MemberID:            session.MemberID(),
			GroupName:           k.groupName,
			Headers:             msg.Headers,
			Value:               msg.Value,
			Timestamp:           msg.Timestamp,
		})

		scopedCtx, cancel := context.WithCancel(session.Context())
		if err := k.subFunc(scopedCtx, streamMsg); err != nil {
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

var _ streams.Reader = ReaderGroup{}

func NewReaderGroup(groupID string, cfg *sarama.Config, clusterAddrs ...string) (ReaderGroup, error) {
	kConsumerGroup, err := sarama.NewConsumerGroup(clusterAddrs, groupID, cfg)
	if err != nil {
		return ReaderGroup{}, err
	}
	return ReaderGroup{
		groupID: groupID,
		client:  kConsumerGroup,
		handlerPool: &sync.Pool{
			New: func() interface{} {
				return kafkaConsumerGroupHandler{
					groupName: groupID,
				}
			},
		},
	}, nil
}

func NewReaderGroupFromClient(groupID string, c sarama.ConsumerGroup) ReaderGroup {
	return ReaderGroup{
		groupID: groupID,
		client:  c,
		handlerPool: &sync.Pool{
			New: func() interface{} {
				return kafkaConsumerGroupHandler{
					groupName: groupID,
				}
			},
		},
	}
}

func (r ReaderGroup) Close() error {
	return r.client.Close()
}

func (r ReaderGroup) Read(ctx context.Context, stream string, subscriberFunc streams.SubscriberFunc) (err error) {
	handler := r.handlerPool.Get().(kafkaConsumerGroupHandler)
	defer func() {
		r.handlerPool.Put(handler)
	}()
	handler.subFunc = subscriberFunc
	for {
		if errConsumer := r.client.Consume(ctx, []string{stream}, handler); errConsumer != nil &&
			errConsumer != sarama.ErrClosedConsumerGroup {
			err = errConsumer
			break
		}
		select {
		case <-ctx.Done():
			log.Print("closing reader")
			return
		case <-r.client.Errors():
			continue
		}
	}
	return err
}

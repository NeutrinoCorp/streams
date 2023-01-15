package kafka

import (
	"context"
	"log"
	"sync"

	"github.com/Shopify/sarama"
	"github.com/neutrinocorp/streams/v2"
)

const (
	OffsetLatestHighWatermark int64 = -3
)

type ReaderPartition struct {
	client     sarama.Consumer
	offset     int64
	partitions []int32
}

var _ streams.Reader = &ReaderPartition{}

// NewReaderPartition allocates a ReaderPartition. If partitions is empty, then all partitions from topic/stream
// will be read.
func NewReaderPartition(c sarama.Consumer, offset int64, partitions ...int32) *ReaderPartition {
	return &ReaderPartition{
		client:     c,
		offset:     offset,
		partitions: partitions,
	}
}

func (r *ReaderPartition) Close() error {
	log.Print("closing partitioned reader")
	return r.client.Close()
}

func (r *ReaderPartition) Read(ctx context.Context, stream string, subscriberFunc streams.SubscriberFunc) (err error) {
	if len(r.partitions) == 0 {
		r.partitions, err = r.client.Partitions(stream)
		if err != nil {
			return
		}
	}

	wg := sync.WaitGroup{}
	wg.Add(len(r.partitions))
	for _, p := range r.partitions {
		go func(partition int32, offset int64) {
			defer wg.Done()
			consumer, errConsume := r.client.ConsumePartition(stream, partition, offset)
			if errConsume != nil {
				err = errConsume
				return
			}
			defer consumer.Close()
			for {
				for msg := range consumer.Messages() {
					streamMsg := newKConsumerMessage(newKConsumerMessageArgs{
						Key:                 msg.Key,
						Offset:              msg.Offset,
						InitialOffset:       offset,
						HighWaterMarkOffset: consumer.HighWaterMarkOffset(),
						Partition:           msg.Partition,
						Headers:             msg.Headers,
						Value:               msg.Value,
						Timestamp:           msg.Timestamp,
					})

					scopedCtx, cancel := context.WithCancel(context.Background())
					if errSub := subscriberFunc(scopedCtx, streamMsg); errSub != nil {
						cancel()
						continue
					}
					cancel()
				}
				select {
				case <-ctx.Done():
					return
				case <-consumer.Errors():
					continue
				}
			}
		}(p, r.offset)
	}
	wg.Wait()
	return
}

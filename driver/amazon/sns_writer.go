package amazon

import (
	"context"
	"sync"
	"sync/atomic"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sns/types"

	"github.com/aws/aws-sdk-go-v2/service/sns"
	"github.com/neutrinocorp/streams"
)

// SnsWriter is the Amazon Web Services Simple Notification Service (SNS) implementation of streams.Writer.
type SnsWriter struct {
	region, accountID string
	client            *sns.Client
}

var _ streams.Writer = SnsWriter{}

// NewSnsWriter allocates a new SnsWriter ready to be used.
func NewSnsWriter(c *sns.Client, accountID, region string) SnsWriter {
	return SnsWriter{
		region:    region,
		accountID: accountID,
		client:    c,
	}
}

func (s SnsWriter) Write(ctx context.Context, message streams.Message) error {
	msgSns, err := MarshalMessage(message)
	if err != nil {
		return err
	}
	_, err = s.client.Publish(ctx, &sns.PublishInput{
		Message:  msgSns,
		TopicArn: NewTopic(s.region, s.accountID, message.Stream),
	})
	return err
}

func (s SnsWriter) WriteBatch(ctx context.Context, messages ...streams.Message) (published uint32, err error) {
	// Map each stream from `messages` to a batch.
	//
	// Therefore, publish each batch to its requested stream.
	batchBuffer := map[string][]types.PublishBatchRequestEntry{}
	for _, msg := range messages {
		rawJSON, errScoped := MarshalMessage(msg)
		if errScoped != nil {
			err = errScoped
			return
		}
		batchBuffer[msg.Stream] = append(batchBuffer[msg.Stream], types.PublishBatchRequestEntry{
			Id:      aws.String(msg.ID),
			Message: rawJSON,
		})
	}

	wg := sync.WaitGroup{}
	wg.Add(len(batchBuffer))
	for stream, batch := range batchBuffer {
		go func(stream string, messages []types.PublishBatchRequestEntry) {
			defer wg.Done()
			_, err = s.client.PublishBatch(ctx, &sns.PublishBatchInput{
				PublishBatchRequestEntries: messages,
				TopicArn:                   NewTopic(s.region, s.accountID, stream),
			})
			if err != nil {
				return
			}
			atomic.AddUint32(&published, 1)
		}(stream, batch)
	}
	wg.Wait()
	return
}

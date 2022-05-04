package amazon

import (
	"context"
	"sync"
	"sync/atomic"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"

	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/neutrinocorp/streams"
)

// SqsWriter is the Amazon Web Services Simple Queue Service (SQS) implementation of streams.Writer.
type SqsWriter struct {
	region, accountID string
	client            *sqs.Client
}

var _ streams.Writer = SqsWriter{}

// NewSqsWriter allocates a new SqsWriter ready to be used.
func NewSqsWriter(c *sqs.Client, accountID, region string) SqsWriter {
	return SqsWriter{
		region:    region,
		accountID: accountID,
		client:    c,
	}
}

func (s SqsWriter) Write(ctx context.Context, message streams.Message) error {
	rawJSON, err := MarshalMessage(message)
	if err != nil {
		return err
	}
	_, err = s.client.SendMessage(ctx, &sqs.SendMessageInput{
		MessageBody: rawJSON,
		QueueUrl:    NewQueueUrl(s.region, s.accountID, message.Stream),
	})
	return err
}

func (s SqsWriter) WriteBatch(ctx context.Context, messages ...streams.Message) (published uint32, err error) {
	// Map each stream from `messages` to a batch.
	//
	// Therefore, publish each batch to its requested stream.
	batchBuffer := map[string][]types.SendMessageBatchRequestEntry{}
	for _, msg := range messages {
		rawJSON, errScoped := MarshalMessage(msg)
		if errScoped != nil {
			err = errScoped
			return
		}
		batchBuffer[msg.Stream] = append(batchBuffer[msg.Stream], types.SendMessageBatchRequestEntry{
			Id:          aws.String(msg.ID),
			MessageBody: rawJSON,
		})
	}

	wg := sync.WaitGroup{}
	wg.Add(len(batchBuffer))
	for stream, batch := range batchBuffer {
		go func(stream string, messages []types.SendMessageBatchRequestEntry) {
			defer wg.Done()
			_, err = s.client.SendMessageBatch(ctx, &sqs.SendMessageBatchInput{
				Entries:  messages,
				QueueUrl: NewQueueUrl(s.region, s.accountID, stream),
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

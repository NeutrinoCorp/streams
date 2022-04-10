package amazon

import (
	"context"
	"sync"
	"sync/atomic"

	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/neutrinocorp/streamhub"
)

// SqsWriter is the Amazon Web Services Simple Queue Service (SQS) implementation of streamhub.Writer.
type SqsWriter struct {
	region, accountID string
	client            *sqs.Client
}

var _ streamhub.Writer = SqsWriter{}

// NewSqsWriter allocates a new SqsWriter ready to be used.
func NewSqsWriter(c *sqs.Client, accountID, region string) SqsWriter {
	return SqsWriter{
		region:    region,
		accountID: accountID,
		client:    c,
	}
}

func (s SqsWriter) Write(ctx context.Context, message streamhub.Message) error {
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

func (s SqsWriter) WriteBatch(ctx context.Context, messages ...streamhub.Message) (published uint32, err error) {
	// Important NOTE: We cannot use AWS API's PublishBatch func as it delivers messages to only one queue target.
	// Each message from this function (SqsWriter.WriteBatch()) contains a `stream` field which is the queue target in
	// this case, hence the batch MIGHT contain multiple streams to target to.
	wg := sync.WaitGroup{}
	wg.Add(len(messages))
	for _, msg := range messages {
		go func(message streamhub.Message) {
			defer wg.Done()
			if errScoped := s.Write(ctx, message); errScoped != nil {
				err = errScoped
				return
			}
			atomic.AddUint32(&published, 1)
		}(msg)
	}
	wg.Wait()
	return
}

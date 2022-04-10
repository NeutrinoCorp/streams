package amazon

import (
	"context"
	"sync"
	"sync/atomic"

	"github.com/aws/aws-sdk-go-v2/service/sns"
	"github.com/neutrinocorp/streamhub"
)

// SnsWriter is the Amazon Web Services Simple Notification Service (SNS) implementation of streamhub.Writer.
type SnsWriter struct {
	region, accountID string
	client            *sns.Client
}

var _ streamhub.Writer = SnsWriter{}

// NewSnsWriter allocates a new SnsWriter ready to be used.
func NewSnsWriter(c *sns.Client, accountID, region string) SnsWriter {
	return SnsWriter{
		region:    region,
		accountID: accountID,
		client:    c,
	}
}

func (s SnsWriter) Write(ctx context.Context, message streamhub.Message) error {
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

func (s SnsWriter) WriteBatch(ctx context.Context, messages ...streamhub.Message) (published uint32, err error) {
	// Important NOTE: We cannot use AWS API's PublishBatch func as it delivers messages to only one topic target.
	// Each message from this function (SnsWriter.WriteBatch()) contains a `stream` field which is the topic target in
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

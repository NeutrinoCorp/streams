//go:build integration

package amazon_test

import (
	"context"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/google/uuid"
	"github.com/neutrinocorp/streams"
	"github.com/neutrinocorp/streams/driver/amazon"
	"github.com/stretchr/testify/suite"
)

var sqsQueues = []string{
	"ncorp-dev-sample_service-action-on-foo-triggered",
	"ncorp-prod-sample_service-fooing-on-bar-triggered",
}

type SqsWriterSuite struct {
	suite.Suite
	accountID string
	cfgAws    aws.Config
	client    *sqs.Client
	writer    amazon.SqsWriter
	marshaler streams.Marshaler

	queueBuffer sync.Map
}

func TestNewSqsWriter(t *testing.T) {
	suite.Run(t, &SqsWriterSuite{})
}

func (s *SqsWriterSuite) SetupSuite() {
	s.accountID = defaultLocalAwsAccountID
	s.cfgAws = defaultLocalAwsConfig
	s.client = sqs.NewFromConfig(s.cfgAws)
	s.queueBuffer = sync.Map{}
	s.createSqsQueues()
	s.writer = amazon.NewSqsWriter(s.client, s.accountID, s.cfgAws.Region)
	s.marshaler = streams.JSONMarshaler{}
}

func (s *SqsWriterSuite) createSqsQueues() {
	var err error
	wg := sync.WaitGroup{}
	wg.Add(len(sqsQueues))
	for i := 0; i < len(sqsQueues); i++ {
		go func(index int) {
			defer wg.Done()
			ctx, cancel := context.WithTimeout(context.Background(), time.Second*8)
			defer cancel()
			out, errScoped := s.client.CreateQueue(ctx, &sqs.CreateQueueInput{
				QueueName: aws.String(sqsQueues[index]),
			})
			if errScoped != nil && !strings.Contains(errScoped.Error(), "TopicAlreadyExists") {
				err = errScoped
				return
			}
			s.queueBuffer.Store(sqsQueues[index], out.QueueUrl)
		}(i)
	}
	wg.Wait()
	s.Require().NoError(err)
}

func (s *SqsWriterSuite) TearDownSuite() {
	s.removeSqsQueues()
}

func (s *SqsWriterSuite) removeSqsQueues() {
	var err error
	wg := sync.WaitGroup{}
	wg.Add(len(sqsQueues))
	for i := 0; i < len(sqsQueues); i++ {
		go func(index int) {
			defer wg.Done()
			rawArn, ok := s.queueBuffer.Load(sqsQueues[index])
			if !ok {
				return
			}
			ctx, cancel := context.WithTimeout(context.Background(), time.Second*8)
			defer cancel()
			_, errScoped := s.client.DeleteQueue(ctx, &sqs.DeleteQueueInput{
				QueueUrl: rawArn.(*string),
			})
			if errScoped != nil {
				err = errScoped
			}
		}(i)
	}
	wg.Wait()
	s.Require().NoError(err)
}

func (s *SqsWriterSuite) TestSqsWriter_Write() {
	eventRaw, err := s.marshaler.Marshal("", sampleEventFoo{
		Foo: "lorem ipsum",
		Bar: "dolor sit amet",
	})
	s.Require().NoError(err)
	err = s.writer.Write(context.Background(), streams.NewMessage(streams.NewMessageArgs{
		SchemaVersion:        1,
		Data:                 eventRaw,
		ID:                   uuid.NewString(),
		Source:               "",
		Stream:               sqsQueues[0],
		SchemaDefinitionName: "",
		ContentType:          s.marshaler.ContentType(),
		Subject:              "",
	}))
	s.Assert().NoError(err)
}

func (s *SqsWriterSuite) TestSqsWriter_WriteBatch() {
	eventRaw, err := s.marshaler.Marshal("", sampleEventFoo{
		Foo: "lorem ipsum",
		Bar: "dolor sit amet",
	})
	s.Require().NoError(err)
	out, err := s.writer.WriteBatch(context.Background(), streams.NewMessage(streams.NewMessageArgs{
		SchemaVersion:        1,
		Data:                 eventRaw,
		ID:                   uuid.NewString(),
		Source:               "",
		Stream:               sqsQueues[0],
		SchemaDefinitionName: "",
		ContentType:          s.marshaler.ContentType(),
		Subject:              "",
	}), streams.NewMessage(streams.NewMessageArgs{
		SchemaVersion:        1,
		Data:                 eventRaw,
		ID:                   uuid.NewString(),
		Source:               "",
		Stream:               sqsQueues[1],
		SchemaDefinitionName: "",
		ContentType:          s.marshaler.ContentType(),
		Subject:              "",
	}))
	s.Assert().NoError(err)
	s.Assert().Equal(uint32(2), out)
}

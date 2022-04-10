//go:build integration

package amazon_test

import (
	"context"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sns"
	"github.com/google/uuid"
	"github.com/neutrinocorp/streams"
	"github.com/neutrinocorp/streams/driver/amazon"
	"github.com/stretchr/testify/suite"
)

var snsTopics = []string{
	"ncorp-dev-sample_service-action"}

type SnsWriterSuite struct {
	suite.Suite
	accountID string
	cfgAws    aws.Config
	client    *sns.Client
	writer    amazon.SnsWriter
	marshaler streams.Marshaler

	topicBuffer sync.Map
}

func TestNewSnsWriter(t *testing.T) {
	suite.Run(t, &SnsWriterSuite{})
}

func (s *SnsWriterSuite) SetupSuite() {
	s.accountID = defaultLocalAwsAccountID
	s.cfgAws = defaultLocalAwsConfig
	s.client = sns.NewFromConfig(s.cfgAws)
	s.topicBuffer = sync.Map{}
	s.createSnsTopics()
	s.writer = amazon.NewSnsWriter(s.client, s.accountID, s.cfgAws.Region)
	s.marshaler = streams.JSONMarshaler{}
}

func (s *SnsWriterSuite) createSnsTopics() {
	var err error
	wg := sync.WaitGroup{}
	wg.Add(len(snsTopics))
	for i := 0; i < len(snsTopics); i++ {
		go func(index int) {
			defer wg.Done()
			ctx, cancel := context.WithTimeout(context.Background(), time.Second*8)
			defer cancel()
			out, errScoped := s.client.CreateTopic(ctx, &sns.CreateTopicInput{
				Name:       aws.String(snsTopics[index]),
				Attributes: nil,
				Tags:       nil,
			})
			if errScoped != nil && !strings.Contains(errScoped.Error(), "TopicAlreadyExists") {
				err = errScoped
				return
			}
			s.topicBuffer.Store(snsTopics[index], out.TopicArn)
		}(i)
	}
	wg.Wait()
	s.Require().NoError(err)
}

func (s *SnsWriterSuite) TearDownSuite() {
	s.removeSnsTopics()
}

func (s *SnsWriterSuite) removeSnsTopics() {
	var err error
	wg := sync.WaitGroup{}
	wg.Add(len(snsTopics))
	for i := 0; i < len(snsTopics); i++ {
		go func(index int) {
			defer wg.Done()
			rawArn, ok := s.topicBuffer.Load(snsTopics[index])
			if !ok {
				return
			}
			ctx, cancel := context.WithTimeout(context.Background(), time.Second*8)
			defer cancel()
			_, errScoped := s.client.DeleteTopic(ctx, &sns.DeleteTopicInput{
				TopicArn: rawArn.(*string),
			})
			if errScoped != nil {
				err = errScoped
			}
		}(i)
	}
	wg.Wait()
	s.Require().NoError(err)
}

func (s *SnsWriterSuite) TestSnsWriter_Write() {
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
		Stream:               snsTopics[0],
		SchemaDefinitionName: "",
		ContentType:          s.marshaler.ContentType(),
		Subject:              "",
	}))
	s.Assert().NoError(err)
}

func (s *SnsWriterSuite) TestSnsWriter_WriteBatch() {
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
		Stream:               snsTopics[0],
		SchemaDefinitionName: "",
		ContentType:          s.marshaler.ContentType(),
		Subject:              "",
	}), streams.NewMessage(streams.NewMessageArgs{
		SchemaVersion:        1,
		Data:                 eventRaw,
		ID:                   uuid.NewString(),
		Source:               "",
		Stream:               snsTopics[0],
		SchemaDefinitionName: "",
		ContentType:          s.marshaler.ContentType(),
		Subject:              "",
	}))
	s.Assert().NoError(err)
	s.Assert().Equal(uint32(2), out)
}

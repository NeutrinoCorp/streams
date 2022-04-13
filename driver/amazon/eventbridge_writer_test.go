//go:build integration

package amazon_test

import (
	"context"
	"sync"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/eventbridge"
	"github.com/neutrinocorp/streams"
	"github.com/neutrinocorp/streams/driver/amazon"
	"github.com/neutrinocorp/streams/testdata/proto/examplepb"
	"github.com/stretchr/testify/suite"
)

const (
	eventBridgeBusName   = "ncorp.dev"
	eventBridgeTopicName = "places.platform.foo"
	eventBridgeSource    = "places.platform.foo"
)

type EventBridgeWriterSuite struct {
	suite.Suite
	accountID string
	cfgAws    aws.Config
	client    *eventbridge.Client
	writer    amazon.EventBridgeWriter
	marshaler streams.Marshaler

	topicBuffer sync.Map
}

func TestNewEventBridgeWriter(t *testing.T) {
	suite.Run(t, &EventBridgeWriterSuite{})
}

func (s *EventBridgeWriterSuite) SetupSuite() {
	s.accountID = defaultLocalAwsAccountID
	s.cfgAws = defaultLocalAwsConfig
	s.client = eventbridge.NewFromConfig(s.cfgAws)
	s.topicBuffer = sync.Map{}
	s.createEventBridgeBus()
	s.writer = amazon.NewEventBridgeWriter(s.client, s.accountID, s.cfgAws.Region, eventBridgeBusName)
	s.marshaler = streams.ProtocolBuffersMarshaler{}
}

func (s *EventBridgeWriterSuite) createEventBridgeBus() {
	_, err := s.client.CreateEventBus(context.Background(), &eventbridge.CreateEventBusInput{
		Name: aws.String(eventBridgeBusName),
	})
	s.Require().NoError(err)
}

func (s *EventBridgeWriterSuite) TearDownSuite() {
	s.removeEventBridgeBus()
}

func (s *EventBridgeWriterSuite) removeEventBridgeBus() {
	_, err := s.client.DeleteEventBus(context.Background(), &eventbridge.DeleteEventBusInput{
		Name: aws.String(eventBridgeBusName),
	})
	s.Require().NoError(err)
}

func (s *EventBridgeWriterSuite) TestEventBridgeWriter_Write() {
	eventRaw, err := s.marshaler.Marshal("", &examplepb.Person{
		Name:  "lorem ipsum",
		Id:    10,
		Email: "dolor amet",
	})
	s.Require().NoError(err)
	err = s.writer.Write(context.Background(), streams.NewMessage(streams.NewMessageArgs{
		SchemaVersion:        1,
		Data:                 eventRaw,
		ID:                   "123",
		Source:               eventBridgeSource,
		Stream:               eventBridgeTopicName,
		SchemaDefinitionName: "",
		ContentType:          s.marshaler.ContentType(),
		Subject:              "",
	}))
	s.Assert().NoError(err)
}

func (s *EventBridgeWriterSuite) TestEventBridgeWriter_WriteBatch() {
	eventRaw, err := s.marshaler.Marshal("", &examplepb.Person{
		Name:  "lorem ipsum",
		Id:    10,
		Email: "dolor amet",
	})
	s.Require().NoError(err)
	out, err := s.writer.WriteBatch(context.Background(), streams.NewMessage(streams.NewMessageArgs{
		SchemaVersion:        1,
		Data:                 eventRaw,
		ID:                   "123",
		Source:               eventBridgeSource,
		Stream:               eventBridgeTopicName,
		SchemaDefinitionName: "",
		ContentType:          s.marshaler.ContentType(),
		Subject:              "",
	}), streams.NewMessage(streams.NewMessageArgs{
		SchemaVersion:        1,
		Data:                 eventRaw,
		ID:                   "456",
		Source:               eventBridgeSource,
		Stream:               eventBridgeTopicName,
		SchemaDefinitionName: "",
		ContentType:          s.marshaler.ContentType(),
		Subject:              "",
	}))
	s.Assert().NoError(err)
	s.Assert().Equal(uint32(2), out)
}

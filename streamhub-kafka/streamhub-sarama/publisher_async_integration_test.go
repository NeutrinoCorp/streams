//go:build integration

package streamhub_sarama_test

import (
	"context"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Shopify/sarama"
	"github.com/neutrinocorp/streamhub"
	kafka "github.com/neutrinocorp/streamhub/streamhub-sarama"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
)

const totalAsyncPublisherJobs = 4

type asyncPublisherTestSuite struct {
	suite.Suite

	client sarama.Client
	admin  sarama.ClusterAdmin
	topic  string

	producer           sarama.AsyncProducer
	totalJobsCompleted uint32
	publishedMessages  *messageAtomicStack
}

func TestAsyncPublisherSuite(t *testing.T) {
	suite.Run(t, &asyncPublisherTestSuite{})
}

func (s *asyncPublisherTestSuite) SetupSuite() {
	s.setupClient()
	s.setupClusterAdmin()
	s.setupSaramaProducer()
	s.topic = studentSuspendedTopic
	s.cleanTopic()
	s.setupTopic()
	s.publishedMessages = &messageAtomicStack{
		stack: make([]messageMetadata, 0),
		mu:    sync.RWMutex{},
	}
}

func (s *asyncPublisherTestSuite) setupClient() {
	var err error
	s.client, err = sarama.NewClient(defaultKafkaClusterAddresses, defaultSaramaConfig)
	if err != nil {
		s.T().Fatal(err)
	}
}

func (s *asyncPublisherTestSuite) setupClusterAdmin() {
	var err error
	s.admin, err = sarama.NewClusterAdminFromClient(s.client)
	if err != nil {
		s.T().Fatal(err)
	}
}

func (s *asyncPublisherTestSuite) setupSaramaProducer() {
	var err error
	s.producer, err = sarama.NewAsyncProducerFromClient(s.client)
	if err != nil {
		s.T().Fatal(err)
	}
}

func (s *asyncPublisherTestSuite) setupTopic() {
	err := s.admin.CreateTopic(s.topic, &sarama.TopicDetail{
		NumPartitions:     defaultTopicPartitions,
		ReplicationFactor: defaultTopicReplicationFactor,
		ReplicaAssignment: nil,
		ConfigEntries:     nil,
	}, false)

	if err != nil && !strings.HasPrefix(err.Error(), "kafka server: Topic with this name already exists. - Topic") {
		s.T().Fatal(err)
	}
}

func (s *asyncPublisherTestSuite) TearDownTest() {
	if total := atomic.LoadUint32(&s.totalJobsCompleted); total < totalAsyncPublisherJobs {
		return
	}

	s.cleanTopic()
	if err := s.producer.Close(); err != nil {
		s.T().Error(err)
	}
	if err := s.admin.Close(); err != nil {
		s.T().Error(err)
	}
	_ = s.client.Close()
}

func (s *asyncPublisherTestSuite) cleanTopic() {
	if s.client.Closed() {
		return
	}

	if err := s.admin.DeleteTopic(s.topic); err != nil {
		s.T().Log(err)
	}
}

func (s *asyncPublisherTestSuite) TestAsyncPublisher_CorrelationID() {
	defer func() {
		atomic.AddUint32(&s.totalJobsCompleted, 1)
	}()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	client, done, ready := setupConsumerGroup(setupConsumerGroupArgs{
		t:                 s.T(),
		baseCtx:           ctx,
		groupSuffix:       "pub_correlation_id_async",
		publishedMessages: s.publishedMessages,
		next: func(message *sarama.ConsumerMessage) error {
			assert.Equal(s.T(), correlationID, string(message.Key))
			return nil
		},
		topic: s.topic,
	})
	defer client.Close()
	<-ready

	hub := newPreconfiguredHubAsync(
		streamhub.WithIDFactory(mockStaticIdGenerator),
		streamhub.WithPublisher(kafka.NewAsyncPublisher(s.producer, nil, nil,
			kafka.PublisherCorrelationIdStrategy)))

	errPub := hub.Publish(ctx, studentApproved{
		StudentID:   "st-1233-es",
		StudentName: "Joe Doe",
		ApprovedAt:  time.Now().UTC().Format(time.RFC3339),
	})
	assert.NoError(s.T(), errPub)
	if errPub != nil {
		s.T().Fatal(errPub)
	}
	waitOperationWithTimeout(s.T(), done, "TestAsyncPublisher_CorrelationID", defaultOperationTimeout)
}

func (s *asyncPublisherTestSuite) TestAsyncPublisher_MessageID() {
	defer func() {
		atomic.AddUint32(&s.totalJobsCompleted, 1)
	}()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	client, done, ready := setupConsumerGroup(setupConsumerGroupArgs{
		t:                 s.T(),
		baseCtx:           ctx,
		groupSuffix:       "pub_message_id_async",
		publishedMessages: s.publishedMessages,
		next: func(message *sarama.ConsumerMessage) error {
			assert.Equal(s.T(), correlationID, string(message.Key))
			return nil
		},
		topic: s.topic,
	})
	defer client.Close()
	<-ready

	hub := newPreconfiguredHubAsync(
		streamhub.WithIDFactory(mockStaticIdGenerator),
		streamhub.WithPublisher(kafka.NewAsyncPublisher(s.producer, nil, nil,
			kafka.PublisherCorrelationIdStrategy)))

	errPub := hub.Publish(ctx, studentApproved{
		StudentID:   "st-1233-es",
		StudentName: "Joe Doe",
		ApprovedAt:  time.Now().UTC().Format(time.RFC3339),
	})
	assert.NoError(s.T(), errPub)
	waitOperationWithTimeout(s.T(), done, "TestAsyncPublisher_MessageID", defaultOperationTimeout)
}

func (s *asyncPublisherTestSuite) TestAsyncPublisher_RoundRobin() {
	defer func() {
		atomic.AddUint32(&s.totalJobsCompleted, 1)
	}()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	client, done, ready := setupConsumerGroup(setupConsumerGroupArgs{
		t:                 s.T(),
		baseCtx:           ctx,
		groupSuffix:       "pub_round_robin_async",
		publishedMessages: s.publishedMessages,
		next: func(message *sarama.ConsumerMessage) error {
			assert.Nil(s.T(), message.Key)
			return nil
		},
		topic: s.topic,
	})
	defer client.Close()
	<-ready

	hub := newPreconfiguredHubAsync(
		streamhub.WithIDFactory(streamhub.RandInt64Factory),
		streamhub.WithPublisher(kafka.NewAsyncPublisher(s.producer, nil, nil,
			kafka.PublisherCorrelationIdStrategy)))

	errPub := hub.Publish(ctx, studentApproved{
		StudentID:   "st-123-abc",
		StudentName: "Joe Doe",
		ApprovedAt:  time.Now().UTC().Format(time.RFC3339),
	})
	assert.NoError(s.T(), errPub)
	waitOperationWithTimeout(s.T(), done, "TestAsyncPublisher_RoundRobin", defaultOperationTimeout)
}

func (s *asyncPublisherTestSuite) TestAsyncPublisher_Subject() {
	defer func() {
		atomic.AddUint32(&s.totalJobsCompleted, 1)
	}()

	const subject = "st-1233-es"
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	client, done, ready := setupConsumerGroup(setupConsumerGroupArgs{
		t:                 s.T(),
		baseCtx:           ctx,
		groupSuffix:       "pub_subject_async",
		publishedMessages: s.publishedMessages,
		next: func(message *sarama.ConsumerMessage) error {
			assert.Equal(s.T(), subject, string(message.Key))
			return nil
		},
		topic: s.topic,
	})
	defer client.Close()
	<-ready

	hub := newPreconfiguredHubAsync(
		streamhub.WithIDFactory(streamhub.RandInt64Factory),
		streamhub.WithPublisher(kafka.NewAsyncPublisher(s.producer, nil, nil,
			kafka.PublisherCorrelationIdStrategy)))

	errPub := hub.Publish(ctx, studentApproved{
		StudentID:   subject,
		StudentName: "Joe Doe",
		ApprovedAt:  time.Now().UTC().Format(time.RFC3339),
	})
	assert.NoError(s.T(), errPub)
	waitOperationWithTimeout(s.T(), done, "TestAsyncPublisher_Subject", defaultOperationTimeout)
}

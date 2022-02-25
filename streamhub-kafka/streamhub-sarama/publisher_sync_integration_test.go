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

const totalSyncPublisherJobs = 4

type syncPublisherTestSuite struct {
	suite.Suite

	client sarama.Client
	admin  sarama.ClusterAdmin
	topic  string

	producer           sarama.SyncProducer
	totalJobsCompleted uint32
	publishedMessages  *messageAtomicStack
}

func TestSyncPublisherSuite(t *testing.T) {
	suite.Run(t, &syncPublisherTestSuite{})
}

func (s *syncPublisherTestSuite) SetupSuite() {
	s.setupClient()
	s.setupClusterAdmin()
	s.setupSaramaProducer()
	s.topic = studentApprovedTopic
	s.cleanTopic()
	s.setupTopic()
	s.publishedMessages = &messageAtomicStack{
		stack: make(map[string]messageMetadata, 0),
		mu:    sync.RWMutex{},
	}
}

func (s *syncPublisherTestSuite) setupClient() {
	var err error
	s.client, err = sarama.NewClient(defaultKafkaClusterAddresses, defaultSaramaConfig)
	if err != nil {
		s.T().Fatal(err)
	}
}

func (s *syncPublisherTestSuite) setupClusterAdmin() {
	var err error
	s.admin, err = sarama.NewClusterAdminFromClient(s.client)
	if err != nil {
		s.T().Fatal(err)
	}
}

func (s *syncPublisherTestSuite) setupSaramaProducer() {
	var err error
	s.producer, err = sarama.NewSyncProducerFromClient(s.client)
	if err != nil {
		s.T().Fatal(err)
	}
}

func (s *syncPublisherTestSuite) setupTopic() {
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

func (s *syncPublisherTestSuite) TearDownTest() {
	if total := atomic.LoadUint32(&s.totalJobsCompleted); total < totalSyncPublisherJobs {
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

func (s *syncPublisherTestSuite) cleanTopic() {
	if s.client.Closed() {
		return
	}

	if err := s.admin.DeleteTopic(s.topic); err != nil {
		s.T().Log(err)
	}
}

func (s *syncPublisherTestSuite) TestSyncPublisher_CorrelationID() {
	defer func() {
		atomic.AddUint32(&s.totalJobsCompleted, 1)
	}()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	client, done, ready := setupConsumerGroup(setupConsumerGroupArgs{
		t:                 s.T(),
		baseCtx:           ctx,
		groupSuffix:       "pub_correlation_id",
		publishedMessages: s.publishedMessages,
		next: func(message *sarama.ConsumerMessage) error {
			assert.Equal(s.T(), correlationID, string(message.Key))
			return nil
		},
		topic: s.topic,
	})
	defer client.Close()
	<-ready

	hub := newPreconfiguredHub(
		streamhub.WithIDFactory(mockStaticIdGenerator),
		streamhub.WithPublisher(kafka.NewSyncPublisher(s.producer, nil,
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
	waitOperationWithTimeout(s.T(), done, "TestSyncPublisher_CorrelationID", defaultOperationTimeout)
}

func (s *syncPublisherTestSuite) TestSyncPublisher_MessageID() {
	defer func() {
		atomic.AddUint32(&s.totalJobsCompleted, 1)
	}()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	client, done, ready := setupConsumerGroup(setupConsumerGroupArgs{
		t:                 s.T(),
		baseCtx:           ctx,
		groupSuffix:       "pub_message_id",
		publishedMessages: s.publishedMessages,
		next: func(message *sarama.ConsumerMessage) error {
			assert.Equal(s.T(), correlationID, string(message.Key))
			return nil
		},
		topic: s.topic,
	})
	defer client.Close()
	<-ready

	hub := newPreconfiguredHub(
		streamhub.WithIDFactory(mockStaticIdGenerator),
		streamhub.WithPublisher(kafka.NewSyncPublisher(s.producer, nil,
			kafka.PublisherMessageIdStrategy)))

	errPub := hub.Publish(ctx, studentApproved{
		StudentID:   "st-1233-es",
		StudentName: "Joe Doe",
		ApprovedAt:  time.Now().UTC().Format(time.RFC3339),
	})
	assert.NoError(s.T(), errPub)
	waitOperationWithTimeout(s.T(), done, "TestSyncPublisher_MessageID", defaultOperationTimeout)
}

func (s *syncPublisherTestSuite) TestSyncPublisher_RoundRobin() {
	defer func() {
		atomic.AddUint32(&s.totalJobsCompleted, 1)
	}()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	client, done, ready := setupConsumerGroup(setupConsumerGroupArgs{
		t:                 s.T(),
		baseCtx:           ctx,
		groupSuffix:       "pub_round_robin",
		publishedMessages: s.publishedMessages,
		next: func(message *sarama.ConsumerMessage) error {
			assert.Nil(s.T(), message.Key)
			return nil
		},
		topic: s.topic,
	})
	defer client.Close()
	<-ready

	hub := newPreconfiguredHub(
		streamhub.WithIDFactory(streamhub.RandInt64Factory),
		streamhub.WithPublisher(kafka.NewSyncPublisher(s.producer, nil,
			kafka.PublisherRoundRobinStrategy)))

	errPub := hub.Publish(ctx, studentApproved{
		StudentID:   "st-123-abc",
		StudentName: "Joe Doe",
		ApprovedAt:  time.Now().UTC().Format(time.RFC3339),
	})
	assert.NoError(s.T(), errPub)
	waitOperationWithTimeout(s.T(), done, "TestSyncPublisher_RoundRobin", defaultOperationTimeout)
}

func (s *syncPublisherTestSuite) TestSyncPublisher_Subject() {
	defer func() {
		atomic.AddUint32(&s.totalJobsCompleted, 1)
	}()

	const subject = "st-1233-es"
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	client, done, ready := setupConsumerGroup(setupConsumerGroupArgs{
		t:                 s.T(),
		baseCtx:           ctx,
		groupSuffix:       "pub_subject",
		publishedMessages: s.publishedMessages,
		next: func(message *sarama.ConsumerMessage) error {
			assert.Equal(s.T(), subject, string(message.Key))
			return nil
		},
		topic: s.topic,
	})
	defer client.Close()
	<-ready

	hub := newPreconfiguredHub(
		streamhub.WithIDFactory(streamhub.RandInt64Factory),
		streamhub.WithPublisher(kafka.NewSyncPublisher(s.producer, nil,
			kafka.PublisherSubjectStrategy)))

	errPub := hub.Publish(ctx, studentApproved{
		StudentID:   subject,
		StudentName: "Joe Doe",
		ApprovedAt:  time.Now().UTC().Format(time.RFC3339),
	})
	assert.NoError(s.T(), errPub)
	waitOperationWithTimeout(s.T(), done, "TestSyncPublisher_Subject", defaultOperationTimeout)
}

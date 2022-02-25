package streamhub_sarama_test

import (
	"context"
	"errors"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Shopify/sarama"
	"github.com/neutrinocorp/streamhub"
)

var (
	defaultKafkaClusterAddresses        = []string{"localhost:9092", "localhost:9093", "localhost:9094"}
	defaultSaramaConfig                 = newSaramaConfig()
	defaultTopicPartitions        int32 = 3
	defaultTopicReplicationFactor int16 = 3
	defaultOperationTimeout             = time.Second * 10

	studentApprovedTopic = "org.ncorp.proton.student.approved"
)

func newSaramaConfig() *sarama.Config {
	cfg := sarama.NewConfig()
	cfg.ClientID = "org.ncorp.streamhub-testing"
	// cfg.Producer.Compression = sarama.CompressionGZIP
	cfg.Producer.Return.Successes = true
	cfg.Producer.RequiredAcks = sarama.WaitForAll
	cfg.Producer.Return.Successes = true
	cfg.Producer.Return.Errors = true
	cfg.Consumer.Return.Errors = true
	cfg.Consumer.Offsets.Initial = sarama.OffsetOldest
	cfg.Consumer.Group.Rebalance.Strategy = sarama.BalanceStrategyRange
	return cfg
}

type studentApproved struct {
	StudentID   string `json:"student_id" avro:"student_id"`
	StudentName string `json:"student_name" avro:"student_name"`
	ApprovedAt  string `json:"approved_at" avro:"approved_at"`
}

var _ streamhub.Event = studentApproved{}

func (s studentApproved) Subject() string {
	return s.StudentID
}

const correlationID = "1234abc"

var mockStaticIdGenerator streamhub.IDFactoryFunc = func() (string, error) {
	return correlationID, nil
}

func newPreconfiguredHub(opts ...streamhub.HubOption) *streamhub.Hub {
	opts = append(opts, streamhub.WithInstanceName(defaultSaramaConfig.ClientID))
	hub := streamhub.NewHub(opts...)
	hub.RegisterStream(studentApproved{}, streamhub.StreamMetadata{
		Stream: studentApprovedTopic,
	})
	return hub
}

type messageMetadata struct {
	Partition int32
	Offset    int64
}

type messageAtomicStack struct {
	stack map[string]messageMetadata
	mu    sync.RWMutex
}

func (m *messageAtomicStack) buildHashKey(partition int32, offset int64) string {
	b := strings.Builder{}
	partitionStr := strconv.Itoa(int(partition))
	offsetStr := strconv.Itoa(int(offset))
	b.WriteString(partitionStr)
	b.WriteString("#")
	b.WriteString(offsetStr)
	return b.String()
}

func (m *messageAtomicStack) set(metadata messageMetadata) {
	m.mu.Lock()
	defer m.mu.Unlock()

	key := m.buildHashKey(metadata.Partition, metadata.Offset)
	m.stack[key] = metadata
}

func (m *messageAtomicStack) hasItem(message *sarama.ConsumerMessage) bool {
	m.mu.RLock()
	defer m.mu.RUnlock()

	key := m.buildHashKey(message.Partition, message.Offset)
	msg, ok := m.stack[key]
	if !ok {
		return false
	} else if message.Partition != msg.Partition && message.Offset != msg.Offset {
		return false
	}

	return true
}

type consumerGroupHandlerMiddleware func(message *sarama.ConsumerMessage) error

type defaultConsumerGroupHandler struct {
	ready chan bool
	t     *testing.T
	next  consumerGroupHandlerMiddleware
}

var _ sarama.ConsumerGroupHandler = &defaultConsumerGroupHandler{}

func (d *defaultConsumerGroupHandler) Setup(_ sarama.ConsumerGroupSession) error {
	close(d.ready)
	return nil
}

func (d *defaultConsumerGroupHandler) Cleanup(_ sarama.ConsumerGroupSession) error {
	return nil
}

func (d *defaultConsumerGroupHandler) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for kMessage := range claim.Messages() {
		if err := d.next(kMessage); err == nil {
			session.MarkMessage(kMessage, "")
			session.Commit()
		}
	}
	return nil
}

type setupConsumerGroupArgs struct {
	t                 *testing.T
	baseCtx           context.Context
	publishedMessages *messageAtomicStack
	groupSuffix       string
	next              consumerGroupHandlerMiddleware
	topic             string
}

// setupConsumerGroup creates and starts a new consumer group with the given arguments.
//
// Returns the underlying consumer group client (so it can get closed), a done channel which indicates when the given
// inner job (args.next) finishes. In addition, it returns a ready channel to indicate when the consumer group
// handler is ready to consume claims (and so messages).
//
// Finally, the consumer will stop when parent context (args.baseCtx) gets cancelled.
//
// NOTE: All arguments are REQUIRED.
func setupConsumerGroup(args setupConsumerGroupArgs) (client sarama.ConsumerGroup, done chan bool, ready chan bool) {
	var err error
	client, err = sarama.NewConsumerGroup(defaultKafkaClusterAddresses,
		defaultSaramaConfig.ClientID+"-"+args.groupSuffix, defaultSaramaConfig)
	if err != nil {
		args.t.Fatal(err)
	}

	done = make(chan bool)
	ready = make(chan bool)
	consumer := defaultConsumerGroupHandler{
		ready: ready,
		t:     args.t,
		next: func(message *sarama.ConsumerMessage) error {
			if args.publishedMessages.hasItem(message) {
				return nil
			}
			defer func() {
				args.publishedMessages.set(messageMetadata{
					Partition: message.Partition,
					Offset:    message.Offset,
				})
				done <- true
			}()
			return args.next(message)
		},
	}

	go func() {
		for {
			err = client.Consume(args.baseCtx, []string{args.topic}, &consumer)
			if err != nil && err.Error() != "kafka: tried to use a consumer group that was closed" {
				args.t.Error(err)
				return
			}
			select {
			case <-args.baseCtx.Done():
				return
			default:
			}
			consumer.ready = make(chan bool)
		}
	}()
	return
}

// waitOperationWithTimeout blocks IO until either the timeout passes or the done channel receives a signal.
// The done channel MUST be used by external goroutines to indicate the actual job was finished.
//
// If timeout is passed, an error will be issued to the given testing.T value.
func waitOperationWithTimeout(t *testing.T, done chan bool, operationName string, timeout time.Duration) {
	ticker := time.NewTicker(timeout)
	defer ticker.Stop()
	for {
		select {
		case <-done:
			return
		case <-ticker.C:
			t.Error(errors.New(operationName + ": operation has timeout"))
			return
		default:
		}
	}
}

package main

import (
	"context"
	"log"
	"math/rand"
	"time"

	"github.com/Shopify/sarama"
	"github.com/neutrinocorp/streamhub"
	shsarama "github.com/neutrinocorp/streamhub/streamhub-sarama"
)

type studentSignedUp struct {
	StudentID  string    `json:"student_id" avro:"student_id"`
	SignedUpAt time.Time `json:"signed_up_at" avro:"signed_up_at"`
}

type studentLoggedIn struct {
	StudentID  string    `json:"student_id"`
	Username   string    `json:"username"`
	LoggedInAt time.Time `json:"logged_in_at"`
}

type aggregateMetricOnStudent struct {
	Gauge int `json:"gauge"`
}

type doSomethingOnAggregate struct {
	Foo string `json:"foo"`
}

var loggingListener streamhub.ListenerBehaviour = func(node *streamhub.ListenerNode, hub *streamhub.Hub, next streamhub.ListenerFunc) streamhub.ListenerFunc {
	log.Printf("[RECEIVED] %s | host: %s", node.Stream, hub.InstanceName)
	log.Printf("[RECEIVED] %s | group: %s", node.Stream, node.Group)
	return func(ctx context.Context, message streamhub.Message) error {
		log.Printf("[RECEIVED] %s | group: %s | message_id: %s", node.Stream, node.Group, message.ID)
		log.Printf("[RECEIVED] %s | group: %s | correlation_id: %s", node.Stream, node.Group, message.CorrelationID)
		log.Printf("[RECEIVED] %s | group: %s | causation_id: %s", node.Stream, node.Group, message.CausationID)
		return next(ctx, message)
	}
}

func main() {
	const hostname = "streamhub-tx-test"
	var hosts = []string{"localhost:9092", "localhost:9093", "localhost:9094"}
	cfg := NewSaramaConfig(hostname)
	client, err := sarama.NewClient(hosts, cfg)
	if err != nil {
		panic(err)
	}
	defer client.Close()

	kafkaProducer, err := sarama.NewSyncProducerFromClient(client)
	if err != nil {
		panic(err)
	}
	defer kafkaProducer.Close()

	admin, err := sarama.NewClusterAdminFromClient(client)
	if err != nil {
		panic(err)
	}
	defer admin.Close()

	hub := streamhub.NewHub(
		streamhub.WithListenerBaseOptions(
			streamhub.WithProviderConfiguration(cfg),
			streamhub.WithHosts(hosts...),
			streamhub.WithGroup(hostname),
		),
		streamhub.WithIDFactory(streamhub.RandInt64Factory),
		streamhub.WithListenerBehaviours(loggingListener),
		streamhub.WithPublisher(shsarama.NewSyncPublisher(kafkaProducer, nil, shsarama.PublisherCorrelationIdStrategy)),
		streamhub.WithListenerDriver(shsarama.NewGroupListenerDriver(nil, nil)))

	hub.RegisterStream(studentSignedUp{}, streamhub.StreamMetadata{
		Stream: "student-signed_up",
	})

	hub.RegisterStream(studentLoggedIn{}, streamhub.StreamMetadata{
		Stream:        "student-logged_in",
		SchemaVersion: 8,
	})

	hub.RegisterStream(aggregateMetricOnStudent{}, streamhub.StreamMetadata{
		Stream: "aggregate_metric-on-student-logged_in",
	})

	hub.RegisterStream(doSomethingOnAggregate{}, streamhub.StreamMetadata{
		Stream: "do_something-on-aggregate",
	})

	InitKafkaTopicMigration(admin, hub)
	_ = hub.Listen(studentSignedUp{},
		streamhub.WithGroup("example-job-on-student-signed_up"),
		streamhub.WithListenerFunc(func(ctx context.Context, message streamhub.Message) error {
			data, ok := message.DecodedData.(studentSignedUp)
			if !ok {
				log.Print("failed to cast reflection-message")
			}
			return hub.Publish(ctx, studentLoggedIn{
				StudentID:  data.StudentID,
				Username:   "aruiz",
				LoggedInAt: time.Now().UTC(),
			})
		}))

	_ = hub.Listen(studentLoggedIn{},
		streamhub.WithGroup("job-on-student-logged_in"),
		streamhub.WithListenerFunc(func(ctx context.Context, message streamhub.Message) error {
			data, ok := message.DecodedData.(studentLoggedIn)
			if !ok {
				log.Print("failed to cast logged in reflection-message")
			}
			log.Printf("message decoded at reflection-based: %+v", data)
			return hub.Publish(ctx, aggregateMetricOnStudent{
				Gauge: rand.Int(),
			})
		}))

	_ = hub.Listen(aggregateMetricOnStudent{},
		streamhub.WithGroup("aggregate-on-student-logged_in"),
		streamhub.WithListenerFunc(func(ctx context.Context, message streamhub.Message) error {
			data, ok := message.DecodedData.(aggregateMetricOnStudent)
			if !ok {
				log.Print("failed")
			}
			log.Printf("gauge: %d", data.Gauge)
			return hub.Publish(ctx, doSomethingOnAggregate{
				Foo: "bar",
			})
		}))

	_ = hub.Listen(doSomethingOnAggregate{},
		streamhub.WithListenerFunc(func(ctx context.Context, message streamhub.Message) error {
			return nil
		}))

	_ = hub.Listen(doSomethingOnAggregate{},
		streamhub.WithGroup("foo-group"),
		streamhub.WithListenerFunc(func(ctx context.Context, message streamhub.Message) error {
			return nil
		}))

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*30)
	defer cancel()
	hub.Start(ctx)

	err = hub.Publish(context.Background(), studentSignedUp{
		StudentID:  "2",
		SignedUpAt: time.Now().UTC(),
	})
	if err != nil {
		panic(err)
	}
	time.Sleep(time.Second * 10)
}

func NewSaramaConfig(hostname string) *sarama.Config {
	cfg := sarama.NewConfig()
	cfg.ClientID = hostname
	cfg.Producer.Compression = sarama.CompressionGZIP
	cfg.Producer.Return.Successes = true
	cfg.Producer.RequiredAcks = sarama.WaitForAll
	cfg.Producer.Return.Successes = true
	cfg.Producer.Return.Errors = true
	cfg.Consumer.Return.Errors = true
	cfg.Consumer.Offsets.AutoCommit.Enable = true
	cfg.Consumer.Offsets.Initial = sarama.OffsetOldest
	cfg.Consumer.Group.Rebalance.Strategy = sarama.BalanceStrategyRange
	return cfg
}

func InitKafkaTopicMigration(admin sarama.ClusterAdmin, h *streamhub.Hub) {
	for _, stream := range h.StreamRegistry {
		_ = admin.CreateTopic(stream.Stream, &sarama.TopicDetail{
			NumPartitions:     3,
			ReplicationFactor: 3,
		}, false)
	}
}

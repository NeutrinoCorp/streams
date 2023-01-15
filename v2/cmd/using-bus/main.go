package main

import (
	"context"
	"log"
	"strings"
	"time"

	"github.com/neutrinocorp/streams/v2/driver/kafka"

	"github.com/Shopify/sarama"
	"github.com/google/uuid"
	"github.com/neutrinocorp/streams/v2"
)

type OrderPlaced struct {
	OrderID      string  `json:"order_id"`
	UserID       string  `json:"user_id"`
	Total        float64 `json:"total"`
	CurrencyCode string  `json:"currency_code"`
	OrderedAt    int64   `json:"ordered_at"`
}

func main() {
	brokerAddrs := []string{"localhost:9092"}
	kCfg := newSaramaConfig()
	kClient, err := sarama.NewClient(brokerAddrs, kCfg)
	if err != nil {
		panic(err)
	}

	kConsumerGroup, err := sarama.NewConsumerGroupFromClient("job-a", kClient)
	if err != nil {
		panic(err)
	}
	//defer kConsumerGroup.Close()

	kProd, err := sarama.NewAsyncProducer(brokerAddrs, kCfg)
	if err != nil {
		panic(err)
	}
	defer kProd.Close()

	bus := streams.NewBus(
		kafka.NewAsyncWriter(kProd),
		kafka.NewReaderGroupFromClient("job-a", kConsumerGroup),
		streams.WithCodec(streams.JSONCodec{}),
		streams.WithIdentifierFactory(streams.GoogleUUIDFactory))
	defer bus.Shutdown()
	bus.Set("neutrino.places.orders.placed.v1", OrderPlaced{},
		streams.WithSchemaURL("events.docs.neutrinocorp.org/v1/orders#OrderPlaced"))

	// READER
	bus.RegisterSubscriber(OrderPlaced{}, func(ctx context.Context, msg streams.Message) error {
		order := msg.DecodedData.(OrderPlaced)
		log.Printf("order_id:%s,user_id:%s,total:%f,currency_code:%s,ordered_at:%d", order.OrderID, order.UserID,
			order.Total, order.CurrencyCode, order.OrderedAt)
		log.Printf("[%s] kafka_initial_offset:%s,kafka_offset:%s,kafka_hwo:%s",
			strings.ToUpper(msg.Headers.Get(kafka.HeaderConsumerGroupName)),
			msg.Headers.Get(kafka.HeaderConsumerInitialOffset),
			msg.Headers.Get(kafka.HeaderOffset),
			msg.Headers.Get(kafka.HeaderConsumerHighWatermarkOffset))
		return nil
	})
	r2, err := kafka.NewReaderGroup("job-b", kCfg, "localhost:9092")
	if err != nil {
		panic(err)
	}
	//defer r2.Close()
	bus.RegisterSubscriber(OrderPlaced{}, func(ctx context.Context, msg streams.Message) error {
		log.Printf("[%s] kafka_initial_offset:%s,kafka_offset:%s,kafka_hwo:%s",
			strings.ToUpper(msg.Headers.Get(kafka.HeaderConsumerGroupName)),
			msg.Headers.Get(kafka.HeaderConsumerInitialOffset),
			msg.Headers.Get(kafka.HeaderOffset),
			msg.Headers.Get(kafka.HeaderConsumerHighWatermarkOffset))
		return nil
	}, streams.WithReader(r2))

	kPartConsumer, err := sarama.NewConsumer(brokerAddrs, kCfg)
	if err != nil {
		panic(err)
	}
	partitionReader := kafka.NewReaderPartition(kPartConsumer, sarama.OffsetNewest)
	bus.RegisterSubscriber(OrderPlaced{}, func(ctx context.Context, msg streams.Message) error {
		log.Print("at partition reader")
		log.Printf("[PARTITIONED] kafka_initial_offset:%s,kafka_offset:%s,kafka_hwo:%s",
			msg.Headers.Get(kafka.HeaderConsumerInitialOffset),
			msg.Headers.Get(kafka.HeaderOffset),
			msg.Headers.Get(kafka.HeaderConsumerHighWatermarkOffset))
		return nil
	}, streams.WithReader(partitionReader))

	bus.Start()
	// PRODUCER

	time.Sleep(time.Second)

	out, err := bus.Publish(context.TODO(), streams.PublishMessageArgs{
		Data: OrderPlaced{
			OrderID:      uuid.NewString(),
			UserID:       "aruiz",
			Total:        99.9,
			CurrencyCode: "USD",
			OrderedAt:    time.Now().UnixMilli(),
		},
		Headers: streams.Headers{
			"app-custom-header": "foo",
		},
	}, streams.PublishMessageArgs{
		Data: OrderPlaced{
			OrderID:      uuid.NewString(),
			UserID:       "br1",
			Total:        229.99,
			CurrencyCode: "USD",
			OrderedAt:    time.Now().Add(time.Hour * 4).UnixMilli(),
		},
		Headers: streams.Headers{
			"app-custom-header": "bar",
		},
	}, streams.PublishMessageArgs{
		Data: OrderPlaced{
			OrderID:      uuid.NewString(),
			UserID:       "vicmolgram",
			Total:        999.99,
			CurrencyCode: "USD",
			OrderedAt:    time.Now().Add(time.Hour * 10).UnixMilli(),
		},
		Headers: streams.Headers{
			"app-custom-header": "baz",
		},
	})
	if err != nil {
		panic(err)
	}
	log.Printf("total bytes out: %d", out)
	time.Sleep(time.Second * 10)
}

func newSaramaConfig() *sarama.Config {
	cfg := sarama.NewConfig()
	cfg.ClientID = "service-a"
	cfg.Consumer.Offsets.Initial = sarama.OffsetOldest
	cfg.Consumer.Return.Errors = true
	cfg.Producer.Return.Successes = true
	cfg.Producer.Return.Errors = true
	return cfg
}

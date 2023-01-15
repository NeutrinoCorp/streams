package main

import (
	"context"
	"log"
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
	kClient, err := sarama.NewClient([]string{"localhost:9092"}, newSaramaConfig())
	if err != nil {
		panic(err)
	}

	kConsumerGroup, err := sarama.NewConsumerGroupFromClient("service-a", kClient)
	if err != nil {
		panic(err)
	}
	defer kConsumerGroup.Close()

	kProd, err := sarama.NewAsyncProducerFromClient(kClient)
	if err != nil {
		panic(err)
	}
	defer kProd.Close()

	bus := streams.NewBus(kafka.NewAsyncWriter(kProd), kafka.NewReaderGroup("service-a", kConsumerGroup))
	defer bus.Shutdown()
	bus.Set("neutrino.places.orders.placed.v1", OrderPlaced{},
		streams.WithSchemaURL("events.docs.neutrinocorp.org/v1/orders#OrderPlaced"))

	// READER
	bus.RegisterSubscriber(OrderPlaced{}, func(ctx context.Context, msg streams.Message) error {
		order := msg.DecodedData.(OrderPlaced)
		log.Printf("order_id:%s,user_id:%s,total:%f,currency_code:%s,ordered_at:%d", order.OrderID, order.UserID,
			order.Total, order.CurrencyCode, order.OrderedAt)
		log.Printf("kafka_initial_offset:%s,kafka_offset:%s,kafka_hwo:%s",
			msg.Headers.Get(kafka.HeaderConsumerInitialOffset),
			msg.Headers.Get(kafka.HeaderOffset),
			msg.Headers.Get(kafka.HeaderConsumerHighWatermarkOffset))
		return nil
	})
	bus.Start()
	// PRODUCER

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
	cfg.Consumer.Offsets.Initial = sarama.OffsetOldest
	cfg.Consumer.Return.Errors = true
	cfg.Producer.Return.Successes = true
	cfg.Producer.Return.Errors = true
	return cfg
}

package main

import (
	"context"
	"github.com/Shopify/sarama"
	"github.com/google/uuid"
	"github.com/neutrinocorp/streams/v2"
	"log"
	"time"
)

type OrderPlaced struct {
	OrderID      string  `json:"order_id"`
	UserID       string  `json:"user_id"`
	Total        float64 `json:"total"`
	CurrencyCode string  `json:"currency_code"`
	OrderedAt    int64   `json:"ordered_at"`
}

func main() {
	reg := streams.NewStreamRegistry()
	reg.Set("neutrino.places.orders.placed.v1", streams.StreamMetadata{
		SchemaURL: "",
	}, OrderPlaced{})
	kClient, err := sarama.NewClient([]string{"localhost:9092"}, newSaramaConfig())
	if err != nil {
		panic(err)
	}
	// READER
	kConsumerGroup, err := sarama.NewConsumerGroupFromClient("service-a", kClient)
	if err != nil {
		panic(err)
	}
	defer kConsumerGroup.Close()
	supervisor := streams.NewSubscriberSupervisor(reg, streams.NewKafkaReader("service-a", kConsumerGroup))
	defer supervisor.Shutdown()
	supervisor.Add(OrderPlaced{}, func(ctx context.Context, msg streams.Message) error {
		order := msg.DecodedData.(OrderPlaced)
		log.Printf("order_id:%s,user_id:%s,total:%f,currency_code:%s,ordered_at:%d", order.OrderID, order.UserID,
			order.Total, order.CurrencyCode, order.OrderedAt)
		log.Printf("kafka_initial_offset:%s,kafka_offset:%s,kafka_hwo:%s",
			msg.Headers[streams.HeaderKafkaConsumerInitialOffset],
			msg.Headers[streams.HeaderKafkaOffset],
			msg.Headers[streams.HeaderKafkaConsumerHighWatermarkOffset])
		return nil
	})
	supervisor.Start()
	// PRODUCER
	kProd, err := sarama.NewAsyncProducerFromClient(kClient)
	if err != nil {
		panic(err)
	}
	pub := streams.NewPublisher(
		streams.WithRegistry(reg),
		streams.WithWriter(streams.NewKafkaAsyncWriter(kProd)))
	defer pub.Close()

	out, err := pub.Publish(context.TODO(), streams.PublishMessageArgs{
		Data: OrderPlaced{
			OrderID:      uuid.NewString(),
			UserID:       "aruiz",
			Total:        99.9,
			CurrencyCode: "USD",
			OrderedAt:    time.Now().UnixMilli(),
		},
		Headers: map[string]string{
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
		Headers: map[string]string{
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
		Headers: map[string]string{
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
	cfg.Producer.Return.Successes = true
	cfg.Producer.Return.Errors = true
	return cfg
}

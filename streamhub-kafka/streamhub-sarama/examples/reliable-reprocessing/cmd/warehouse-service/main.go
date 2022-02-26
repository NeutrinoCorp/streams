package main

import (
	"context"
	"errors"
	"log"
	"os"
	"os/signal"
	"reliable-reprocessing/event"
	"reliable-reprocessing/factory"

	"github.com/Shopify/sarama"
	"github.com/neutrinocorp/streamhub"
)

const hostname = "org.ncorp.places.warehouse"

func main() {
	hosts := []string{"localhost:9092", "localhost:9093", "localhost:9093"}
	saramaCfg := factory.NewSaramaConfig(hostname)
	client, err := sarama.NewClient(hosts, saramaCfg)
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

	hub := factory.NewHub(saramaCfg, kafkaProducer, hosts, hostname)
	factory.InitKafkaTopicMigration(admin, hub)
	registerListeners(hub)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() {
		hub.Start(ctx)
	}()

	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)
	<-c
}

func registerListeners(h *streamhub.Hub) {
	err := h.Listen(event.ProductPaid{},
		streamhub.WithGroup(hostname+".allocate_product.on.product_paid"),
		streamhub.WithDeadLetterStream(hostname+".allocate_product.on.product_paid.dlq"),
		streamhub.WithListenerFunc(func(ctx context.Context, message streamhub.Message) error {
			productPaid, ok := message.DecodedData.(event.ProductPaid)
			if ok {
				log.Printf("%+v", productPaid)
			}
			// panic(errors.New("go to DLQ"))
			return errors.New("go to retry or DLQ if not exists")
		}))
	if err != nil {
		panic(err)
	}
}

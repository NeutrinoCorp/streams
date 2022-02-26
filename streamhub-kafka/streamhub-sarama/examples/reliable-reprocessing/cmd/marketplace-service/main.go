package main

import (
	"context"
	"os"
	"os/signal"
	"reliable-reprocessing/event"
	"reliable-reprocessing/factory"
	"time"

	"github.com/Shopify/sarama"
)

const hostname = "org.ncorp.places.marketplace"

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
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() {
		hub.Start(ctx)
	}()

	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)
	time.Sleep(time.Second * 3)
	err = hub.Publish(ctx, event.ProductPaid{
		ID:          "abc",
		SKU:         "qwerty-uiop-asdf",
		DisplayName: "Xbox Series X Controller - Robot White",
		Quantity:    2,
		OrderedAt:   time.Now().UTC().Format(time.RFC3339),
		PaidAt:      time.Now().UTC().Format(time.RFC3339),
	})
	if err != nil {
		panic(err)
	}
	<-c
}

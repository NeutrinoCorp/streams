package factory

import (
	"reliable-reprocessing/event"

	"github.com/Shopify/sarama"
	"github.com/neutrinocorp/streamhub"
	shsarama "github.com/neutrinocorp/streamhub/streamhub-sarama"
)

func NewHub(cfg *sarama.Config, kafkaProducer sarama.SyncProducer, addrs []string, hostname string) *streamhub.Hub {
	hub := streamhub.NewHub(
		streamhub.WithListenerBaseOptions(
			streamhub.WithProviderConfiguration(cfg),
			streamhub.WithHosts(addrs...),
		),
		streamhub.WithSchemaRegistry(newSchemaRegistry()),
		streamhub.WithMarshaler(streamhub.NewAvroMarshaler()),
		streamhub.WithPublisher(shsarama.NewSyncPublisher(kafkaProducer, nil, shsarama.PublisherSubjectStrategy)),
		streamhub.WithInstanceName(hostname),
		streamhub.WithListenerDriver(shsarama.NewGroupListenerDriver(kafkaProducer, nil)),
	)
	initStreamMigration(hub)
	return hub
}

func newSchemaRegistry() streamhub.InMemorySchemaRegistry {
	r := streamhub.InMemorySchemaRegistry{}
	r.RegisterDefinition("org.ncorp.places.marketplace.product.paid", `{
		"type": "record",
		"name": "org.ncorp.places.marketplace.product.paid",
		"namespace": "org.ncorp.avro",
		"fields" : [
			{"name": "product_id", "type": "string"},
			{"name": "product_sku", "type": "string"},
			{"name": "display_name", "type": "string"},
			{"name": "quantity", "type": "int"},
			{"name": "ordered_at", "type": "string"},
			{"name": "paid_at", "type": "string"}
		]
	}`, 0)
	return r
}

func initStreamMigration(hub *streamhub.Hub) {
	hostname := "org.ncorp.places.marketplace"
	hub.RegisterStream(event.ProductPaid{}, streamhub.StreamMetadata{
		Stream: NewStreamName(hostname, "product", "paid"),
	})
}

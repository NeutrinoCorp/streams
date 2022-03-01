package factory

import (
	"github.com/Shopify/sarama"
	"github.com/neutrinocorp/streamhub"
)

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
	_ = admin.CreateTopic("org.ncorp.places.warehouse.allocate_product.on.product_paid.dlq", &sarama.TopicDetail{
		NumPartitions:     3,
		ReplicationFactor: 3,
	}, false)
}

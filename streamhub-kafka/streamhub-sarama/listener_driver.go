package streamhub_sarama

import (
	"context"

	"github.com/Shopify/sarama"
	"github.com/neutrinocorp/streamhub"
)

type ShardedListenerDriver struct {
	c sarama.Consumer
}

var _ streamhub.ListenerDriver = ShardedListenerDriver{}

func (s ShardedListenerDriver) ExecuteTask(_ context.Context, task streamhub.ListenerTask) error {
	_, ok := task.Configuration.(*sarama.Config)
	if !ok {
		return streamhub.ErrInvalidProviderConfiguration
	}

	return nil
}

type GroupListenerDriver struct {
}

var _ streamhub.ListenerDriver = GroupListenerDriver{}

func (g GroupListenerDriver) ExecuteTask(_ context.Context, _ streamhub.ListenerTask) error {
	panic("implement me")
}

package streamhub_sarama_test

import (
	"context"
	"errors"
	"runtime"
	"testing"
	"time"

	jsoniter "github.com/json-iterator/go"

	"github.com/Shopify/sarama"
	"github.com/neutrinocorp/streamhub"
	shsarama "github.com/neutrinocorp/streamhub/streamhub-sarama"
	"github.com/stretchr/testify/assert"
)

var shardListenerTests = []struct {
	InFailConsumerPartition bool
	InMockPartitionConsumer *mockPartitionConsumer
	InMarshaler             streamhub.Marshaler
	InProviderConfig        interface{}
	InConsumerMessage       *sarama.ConsumerMessage
	InPanic                 bool
	ExpErr                  error
	ExpMaxGoroutines        int
}{
	{
		InMockPartitionConsumer: nil,
		InMarshaler:             nil,
		InProviderConfig:        nil,
		InConsumerMessage:       nil,
		ExpErr:                  streamhub.ErrInvalidProviderConfiguration,
		ExpMaxGoroutines:        0,
	},
	{
		InMockPartitionConsumer: newMockPartitonConsumer(),
		InMarshaler:             nil,
		InProviderConfig:        nil,
		InConsumerMessage:       nil,
		ExpErr:                  streamhub.ErrInvalidProviderConfiguration,
		ExpMaxGoroutines:        0,
	},
	{
		InFailConsumerPartition: true,
		InMockPartitionConsumer: newMockPartitonConsumer(),
		InMarshaler:             nil,
		InProviderConfig:        shsarama.ShardListenerConfiguration{},
		InConsumerMessage:       nil,
		ExpErr:                  errors.New("generic error"),
		ExpMaxGoroutines:        0,
	},
	{
		InMockPartitionConsumer: newMockPartitonConsumer(),
		InMarshaler:             streamhub.FailingMarshalerNoop{},
		InProviderConfig:        shsarama.ShardListenerConfiguration{},
		InConsumerMessage: &sarama.ConsumerMessage{
			Value: fakeJSONEvent(),
		},
		ExpErr:           nil,
		ExpMaxGoroutines: 4,
	},
	{
		InMockPartitionConsumer: newMockPartitonConsumer(),
		InMarshaler:             streamhub.JSONMarshaler{},
		InProviderConfig:        shsarama.ShardListenerConfiguration{},
		InConsumerMessage: &sarama.ConsumerMessage{
			Value: fakeJSONEvent(),
		},
		ExpErr:           nil,
		ExpMaxGoroutines: 4,
	},
	{
		InMockPartitionConsumer: newMockPartitonConsumer(),
		InMarshaler:             nil,
		InProviderConfig:        shsarama.ShardListenerConfiguration{},
		InConsumerMessage: &sarama.ConsumerMessage{
			Value: []byte("foo"),
		},
		InPanic:          true,
		ExpErr:           nil,
		ExpMaxGoroutines: 4,
	},
	{
		InMockPartitionConsumer: newMockPartitonConsumer(),
		InMarshaler:             nil,
		InProviderConfig:        shsarama.ShardListenerConfiguration{},
		InConsumerMessage:       nil,
		InPanic:                 true,
		ExpErr:                  nil,
		ExpMaxGoroutines:        4,
	},
	{
		InMockPartitionConsumer: newMockPartitonConsumer(),
		InMarshaler:             nil,
		InProviderConfig:        shsarama.ShardListenerConfiguration{},
		InConsumerMessage: &sarama.ConsumerMessage{
			Value: []byte("foo"),
		},
		ExpErr:           nil,
		ExpMaxGoroutines: 4,
	},
}

func fakeJSONEvent() []byte {
	eventJSON, _ := jsoniter.Marshal(streamhub.Message{
		ID:                "",
		Stream:            "",
		Source:            "",
		SpecVersion:       "",
		Type:              "",
		Data:              []byte("foo"),
		DataContentType:   "",
		DataSchema:        "",
		DataSchemaVersion: 0,
		Timestamp:         "",
		Subject:           "",
		CorrelationID:     "",
		CausationID:       "",
		DecodedData:       nil,
		GroupName:         "",
	})
	return eventJSON
}

func TestShardListenerDriver_ExecuteTask(t *testing.T) {
	for _, tt := range shardListenerTests {
		t.Run("", func(t *testing.T) {
			done := make(chan struct{})

			listener := shsarama.NewShardListenerDriver(fakeConsumer{
				willFail:          tt.InFailConsumerPartition,
				partitionConsumer: tt.InMockPartitionConsumer,
			}, tt.InMarshaler)
			err := listener.ExecuteTask(context.Background(), &streamhub.ListenerNode{
				HandlerFunc: func(ctx context.Context, message streamhub.Message) error {
					if tt.InPanic {
						panic(errors.New("foo"))
					} else if tt.InMarshaler != nil {
						assert.Equal(t, tt.InConsumerMessage.Value, fakeJSONEvent())
					} else {
						assert.Equal(t, tt.InConsumerMessage.Value, message.Data)
					}
					done <- struct{}{}
					return nil
				},
				ProviderConfiguration: tt.InProviderConfig,
			})
			if err != nil {
				assert.Equal(t, tt.ExpErr, err)
				return
			}
			assert.Equal(t, tt.ExpMaxGoroutines, runtime.NumGoroutine())

			tt.InMockPartitionConsumer.messageChan <- tt.InConsumerMessage

			timeout := time.NewTicker(time.Millisecond * 500)
			select {
			case <-timeout.C:
				break
			case <-done:
				break
			}

			_ = tt.InMockPartitionConsumer.Close()
			time.Sleep(time.Millisecond * 500)
			assert.Equal(t, tt.ExpMaxGoroutines-1, runtime.NumGoroutine())
		})
	}
}

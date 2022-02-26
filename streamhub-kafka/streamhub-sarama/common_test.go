package streamhub_sarama_test

import (
	"errors"

	"github.com/Shopify/sarama"
)

type mockPartitionConsumer struct {
	messageChan chan *sarama.ConsumerMessage
}

var _ sarama.PartitionConsumer = &mockPartitionConsumer{}

func newMockPartitonConsumer() *mockPartitionConsumer {
	return &mockPartitionConsumer{messageChan: make(chan *sarama.ConsumerMessage)}
}

func (f *mockPartitionConsumer) AsyncClose() {}

func (f *mockPartitionConsumer) Close() error {
	close(f.messageChan)
	return nil
}

func (f *mockPartitionConsumer) Messages() <-chan *sarama.ConsumerMessage {
	return f.messageChan
}

func (f *mockPartitionConsumer) Errors() <-chan *sarama.ConsumerError {
	return nil
}

func (f *mockPartitionConsumer) HighWaterMarkOffset() int64 {
	return 0
}

func (f *mockPartitionConsumer) Pause() {}

func (f *mockPartitionConsumer) Resume() {}

func (f *mockPartitionConsumer) IsPaused() bool {
	return false
}

type fakeConsumer struct {
	partitionConsumer sarama.PartitionConsumer
	willFail          bool
}

var _ sarama.Consumer = fakeConsumer{}

func (f fakeConsumer) Topics() ([]string, error) {
	return nil, nil
}

func (f fakeConsumer) Partitions(_ string) ([]int32, error) {
	return nil, nil
}

func (f fakeConsumer) ConsumePartition(_ string, _ int32, _ int64) (sarama.PartitionConsumer, error) {
	if f.willFail {
		return nil, errors.New("generic error")
	}
	return f.partitionConsumer, nil
}

func (f fakeConsumer) HighWaterMarks() map[string]map[int32]int64 {
	return nil
}

func (f fakeConsumer) Close() error {
	return nil
}

func (f fakeConsumer) Pause(_ map[string][]int32) {}

func (f fakeConsumer) Resume(_ map[string][]int32) {}

func (f fakeConsumer) PauseAll() {}

func (f fakeConsumer) ResumeAll() {}

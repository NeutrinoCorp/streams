package streamhub_sarama_test

import (
	"testing"

	shsarama "github.com/neutrinocorp/streamhub/streamhub-sarama"
)

func TestGroupListenerDriver_ExecuteTask(t *testing.T) {
	listener := shsarama.NewGroupListenerDriver(nil, nil)

	listener.ExecuteTask(nil, nil)
}

package streamhub_sarama

import (
	"context"
	"errors"

	"github.com/Shopify/sarama"
	"github.com/neutrinocorp/streamhub"
)

// use it to avoid message reprocessing, message goes directly to dead-letter queue if reliable reprocessing was enabled
var errNonRetryableProcess = errors.New("streamhub-sarama: Recovered from panicking process or detected a non-retryable process")

// processMessage executes base driver operations for both ShardListenerDriver and GroupListenerDriver.
//
// It unmarshalls the given sarama.ConsumerMessage, using the given streamhub.Marshaler or Apache Kafka headers, and
// executes the task.HandlerFunc operation with a timeout scoped context
func processMessage(ctx context.Context, marshaler streamhub.Marshaler,
	node *streamhub.ListenerNode, kMessage *sarama.ConsumerMessage) (err error) {
	if kMessage == nil {
		return nil
	}

	defer func() {
		// avoids whole system to collapse because a failing listener node
		if r := recover(); r != nil {
			err = errNonRetryableProcess
		}
	}()
	var message streamhub.Message
	if marshaler != nil {
		if err = marshaler.Unmarshal("", kMessage.Value, &message); err != nil {
			return errNonRetryableProcess
		}
	} else {
		message = deserializeKHeaders(kMessage.Headers)
		message.Data = kMessage.Value
	}

	scopedCtx, cancel := context.WithTimeout(ctx, node.RetryTimeout)
	defer cancel()
	return node.HandlerFunc(scopedCtx, message)
}

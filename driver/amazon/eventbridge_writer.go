package amazon

import (
	"context"
	"errors"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/eventbridge"
	"github.com/aws/aws-sdk-go-v2/service/eventbridge/types"
	"github.com/neutrinocorp/streams"
)

// EventBridgeWriter is the Amazon Web Services EventBridge (formerly known as CloudWatch Events) implementation
// of streams.Writer.
type EventBridgeWriter struct {
	busArn *string
	client *eventbridge.Client
}

var _ streams.Writer = EventBridgeWriter{}

// NewEventBridgeWriter allocates a new EventBridgeWriter ready to be used.
func NewEventBridgeWriter(c *eventbridge.Client, accountID, region, busName string) EventBridgeWriter {
	return EventBridgeWriter{
		busArn: NewEventBusArn(region, accountID, busName),
		client: c,
	}
}

func (e EventBridgeWriter) Write(ctx context.Context, message streams.Message) error {
	_, err := e.WriteBatch(ctx, message)
	return err
}

func (e EventBridgeWriter) WriteBatch(ctx context.Context, messages ...streams.Message) (uint32, error) {
	entries := newEventBridgeMessageBatch(e.busArn, messages)
	o, err := e.client.PutEvents(ctx, &eventbridge.PutEventsInput{
		Entries: entries,
	})
	if err != nil {
		return 0, err
	}
	successfulWrites := uint32(len(messages)) - uint32(o.FailedEntryCount)
	for _, entry := range o.Entries {
		if entry.ErrorMessage != nil {
			return successfulWrites, errors.New(*entry.ErrorMessage)
		}
	}
	return successfulWrites, nil
}

func newEventBridgeMessageBatch(busArn *string, messages []streams.Message) []types.PutEventsRequestEntry {
	if len(messages) == 0 {
		return nil
	}

	entries := make([]types.PutEventsRequestEntry, 0, len(messages))
	for _, msg := range messages {
		rawMsg, err := MarshalMessage(msg)
		if err != nil {
			return nil
		}
		entries = append(entries, types.PutEventsRequestEntry{
			Detail:       rawMsg,
			DetailType:   aws.String(msg.Stream),
			EventBusName: busArn,
			Source:       aws.String(msg.Source),
			Time:         aws.Time(time.Now().UTC()),
		})
	}
	return entries
}

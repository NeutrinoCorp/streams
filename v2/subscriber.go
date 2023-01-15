package streams

import (
	"context"
)

// SubscriberFunc handler to be executed when a new message arrives to the system.
//
// When a nil error is returned, underlying messaging systems with Ack mechanisms WILL send an acknowledgement signal
// to a messaging node.
type SubscriberFunc func(ctx context.Context, msg Message) error

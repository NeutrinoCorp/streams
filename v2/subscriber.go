package streams

import (
	"context"
)

type SubscriberFunc func(ctx context.Context, msg Message) error

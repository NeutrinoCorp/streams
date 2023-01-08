package streams

import (
	"context"
)

type Reader interface {
	Read(ctx context.Context, stream string, subscriberFunc SubscriberFunc) (err error)
}

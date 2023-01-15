package streams

import (
	"context"
	"io"
)

type Reader interface {
	io.Closer
	Read(ctx context.Context, stream string, subscriberFunc SubscriberFunc) (err error)
}

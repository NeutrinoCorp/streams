package streams

import (
	"context"
	"io"
)

type Writer interface {
	io.Closer
	Write(ctx context.Context, p Message) (n int, err error)
	WriteMany(ctx context.Context, p []Message) (n int, err error)
}

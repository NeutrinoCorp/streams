package amazon

import (
	"context"

	"github.com/neutrinocorp/streams"
)

type SqsReader struct {
}

var _ streams.Reader = SqsReader{}

func (s SqsReader) ExecuteTask(_ context.Context, _ streams.ReaderTask) error {
	return nil
}

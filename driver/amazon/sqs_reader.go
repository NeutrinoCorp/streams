package amazon

import (
	"context"

	"github.com/neutrinocorp/streamhub"
)

type SqsReader struct {
}

var _ streamhub.Reader = SqsReader{}

func (s SqsReader) ExecuteTask(_ context.Context, _ streamhub.ReaderTask) error {
	return nil
}

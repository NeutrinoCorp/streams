package streamhub_test

import (
	"testing"

	"github.com/neutrinocorp/streamhub"
	"github.com/stretchr/testify/assert"
)

func TestReaderHandlerNoop(t *testing.T) {
	r := streamhub.ReaderHandlerNoop{}
	assert.Implements(t, (*streamhub.ReaderHandler)(nil), r)
	err := r.Read(nil, streamhub.Message{})
	assert.NoError(t, err)
}

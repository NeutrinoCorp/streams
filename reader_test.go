package streams_test

import (
	"testing"

	"github.com/neutrinocorp/streams"
	"github.com/stretchr/testify/assert"
)

func TestReaderHandlerNoop(t *testing.T) {
	r := streams.ReaderHandlerNoop{}
	assert.Implements(t, (*streams.ReaderHandler)(nil), r)
	err := r.Read(nil, streams.Message{})
	assert.NoError(t, err)
}

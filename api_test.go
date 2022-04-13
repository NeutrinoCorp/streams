package streams_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/neutrinocorp/streams"
)

func TestApi(t *testing.T) {
	defer func() {
		err, ok := recover().(error)
		require.True(t, ok)
		assert.Equal(t, streams.ErrNilDefaultHub, err)
	}()
	streams.DefaultHub = streams.NewHub()
	streams.RegisterStream(fooEvent{}, streams.StreamMetadata{})
	streams.RegisterStreamByString("", streams.StreamMetadata{})
	_ = streams.Read(fooEvent{})
	streams.ReadByStreamKey("")
	_ = streams.Write(nil, fooEvent{})
	_ = streams.WriteByMessageKey(nil, "", fooEvent{})
	_, _ = streams.WriteBatch(nil)
	_, _ = streams.WriteByMessageKeyBatch(nil, nil)
	_ = streams.WriteRawMessage(nil, streams.Message{})
	_, _ = streams.WriteRawMessageBatch(nil, streams.Message{})
	_ = streams.GetStreamReaderNodes("")
	streams.Start(nil)
	streams.DefaultHub = nil
	_ = streams.Read(nil)
}

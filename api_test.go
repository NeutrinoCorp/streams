package streams_test

import (
	"testing"

	"github.com/neutrinocorp/streams"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRead(t *testing.T) {
	defer func() {
		err, ok := recover().(error)
		require.True(t, ok)
		assert.Equal(t, streams.ErrNilDefaultHub, err)
	}()
	_ = streams.Read(nil)
}

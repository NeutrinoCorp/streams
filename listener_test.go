package streamhub_test

import (
	"testing"

	"github.com/neutrinocorp/streamhub"
	"github.com/stretchr/testify/assert"
)

func TestListenerNoop(t *testing.T) {
	l := streamhub.ListenerNoop{}
	assert.Implements(t, (*streamhub.Listener)(nil), l)
	err := l.Listen(nil, streamhub.Message{})
	assert.NoError(t, err)
}

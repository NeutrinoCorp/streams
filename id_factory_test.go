package streamhub_test

import (
	"testing"

	"github.com/neutrinocorp/streamhub"
	"github.com/stretchr/testify/assert"
)

func TestGenerateUUID(t *testing.T) {
	id, err := streamhub.UuidIdFactory()
	assert.NotEmpty(t, id)
	assert.NoError(t, err)
}

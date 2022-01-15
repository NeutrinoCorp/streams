package streamhub_test

import (
	"errors"
	"testing"

	"github.com/neutrinocorp/streamhub"
	"github.com/stretchr/testify/assert"
)

var failingFakeIDFactory streamhub.IDFactoryFunc = func() (string, error) {
	return "", errors.New("generic id factory error")
}

func TestGenerateUUID(t *testing.T) {
	id, err := streamhub.UuidIdFactory()
	assert.NotEmpty(t, id)
	assert.NoError(t, err)
}

package streams_test

import (
	"errors"
	"testing"

	"github.com/neutrinocorp/streams"
	"github.com/stretchr/testify/assert"
)

var failingFakeIDFactory streams.IDFactoryFunc = func() (string, error) {
	return "", errors.New("generic id factory error")
}

func TestGenerateUUID(t *testing.T) {
	id, err := streams.UuidIdFactory()
	assert.NotEmpty(t, id)
	assert.NoError(t, err)
}

func BenchmarkGenerateUUID(b *testing.B) {
	for i := 0; i < b.N; i++ {
		b.ReportAllocs()
		_, _ = streams.UuidIdFactory()
	}
}

func TestGenerateRandID(t *testing.T) {
	id, err := streams.RandInt64Factory()
	assert.NotEmpty(t, id)
	assert.NoError(t, err)
}

func BenchmarkGenerateRandID(b *testing.B) {
	for i := 0; i < b.N; i++ {
		b.ReportAllocs()
		_, _ = streams.RandInt64Factory()
	}
}

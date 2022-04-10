package streams

import (
	"math/rand"
	"strconv"

	"github.com/google/uuid"
)

// IDFactoryFunc creates an unique identifier.
type IDFactoryFunc func() (string, error)

// UuidIdFactory creates a unique identifier using UUID v4 algorithm.
var UuidIdFactory IDFactoryFunc = func() (string, error) {
	id, err := uuid.NewUUID()
	return id.String(), err
}

// RandInt64Factory creates a unique identifier using math/rand built-in package with 64-bit signed integer format
var RandInt64Factory IDFactoryFunc = func() (string, error) {
	i := rand.Int63()
	return strconv.Itoa(int(i)), nil
}

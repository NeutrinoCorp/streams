package streamhub

import "github.com/google/uuid"

// IDFactoryFunc creates an unique identifier.
type IDFactoryFunc func() (string, error)

// UuidIdFactory creates a unique identifier using UUID v4 algorithm.
var UuidIdFactory IDFactoryFunc = func() (string, error) {
	id, err := uuid.NewUUID()
	return id.String(), err
}

package streams

import "github.com/google/uuid"

type IdentifierFactoryFunc func() (string, error)

var GoogleUUIDFactory IdentifierFactoryFunc = func() (string, error) {
	id, err := uuid.NewUUID()
	if err != nil {
		return "", err
	}
	return id.String(), nil
}

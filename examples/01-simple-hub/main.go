package main

import (
	"context"
	"time"

	"github.com/neutrinocorp/streamhub"
)

type StudentSignedUp struct {
	StudentID  string
	SignedUpAt time.Time
}

func main() {
	hub := streamhub.NewHub()
	hub.StreamRegistry.Set(StudentSignedUp{}, streamhub.StreamMetadata{
		Stream:           "student-signed_up",
		SchemaDefinition: "./schemas/student-signed_up.avsc",
		SchemaVersion:    1,
	})

	err := hub.Publish(context.Background(), StudentSignedUp{
		StudentID:  "1",
		SignedUpAt: time.Now().UTC(),
	})
	if err != nil {
		panic(err)
	}
}

package main

import (
	"context"
	"time"

	"github.com/neutrinocorp/streamhub"
)

type studentSignedUp struct {
	StudentID  string    `json:"student_id" avro:"student_id"`
	SignedUpAt time.Time `json:"signed_up_at" avro:"signed_up_at"`
}

func main() {
	hub := streamhub.NewHub()
	hub.StreamRegistry.Set(studentSignedUp{}, streamhub.StreamMetadata{
		Stream: "student-signed_up",
	})

	err := hub.Publish(context.Background(), studentSignedUp{
		StudentID:  "2",
		SignedUpAt: time.Now().UTC(),
	})
	if err != nil {
		panic(err)
	}
}

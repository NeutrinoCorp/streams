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
	hub.RegisterStream(studentSignedUp{}, streamhub.StreamMetadata{
		Stream: "student-signed_up",
	})

	_ = hub.Listen(studentSignedUp{},
		streamhub.WithConsumerGroup("example-job-on-student-signed_up"),
		streamhub.WithListenerFunc(func(ctx context.Context, message streamhub.Message) error {
			return nil
		}))
	hub.ListenByStreamKey("student-signed_up",
		streamhub.WithProviderConfiguration(nil))

	err := hub.Publish(context.Background(), studentSignedUp{
		StudentID:  "2",
		SignedUpAt: time.Now().UTC(),
	})
	if err != nil {
		panic(err)
	}
}

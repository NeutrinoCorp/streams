package main

import (
	"context"
	"log"
	"time"

	"github.com/neutrinocorp/streamhub"
	"github.com/neutrinocorp/streamhub/shmemory"
)

type studentSignedUp struct {
	StudentID  string    `json:"student_id" avro:"student_id"`
	SignedUpAt time.Time `json:"signed_up_at" avro:"signed_up_at"`
}

func main() {
	inMemBus := shmemory.NewBus()
	inMemListener := shmemory.NewListener(inMemBus)
	hub := streamhub.NewHub(
		streamhub.WithPublisher(shmemory.NewPublisher(inMemBus)),
		streamhub.WithBaseDriver(inMemListener))

	hub.RegisterStream(studentSignedUp{}, streamhub.StreamMetadata{
		Stream: "student-signed_up",
	})

	_ = hub.Listen(studentSignedUp{},
		streamhub.WithGroup("example-job-on-student-signed_up"),
		streamhub.WithListenerFunc(func(ctx context.Context, message streamhub.Message) error {
			log.Printf("message decoded: %+v", message.DecodedData)
			return nil
		}))
	hub.ListenByStreamKey("student-signed_up",
		streamhub.WithGroup("example-job-on-student-signed_up"),
		streamhub.WithListenerFunc(func(ctx context.Context, message streamhub.Message) error {
			log.Printf("message decoded: %+v", message.DecodedData)
			return nil
		}))

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*30)
	defer cancel()
	hub.Start(ctx)

	err := hub.Publish(context.Background(), studentSignedUp{
		StudentID:  "2",
		SignedUpAt: time.Now().UTC(),
	})
	if err != nil {
		panic(err)
	}
	time.Sleep(time.Second * 10)
}

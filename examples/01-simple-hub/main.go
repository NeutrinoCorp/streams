package main

import (
	"context"
	"errors"
	"log"
	"time"

	"github.com/neutrinocorp/streamhub"
	shmemory "github.com/neutrinocorp/streamhub/streamhub-memory"
)

type studentSignedUp struct {
	StudentID  string    `json:"student_id" avro:"student_id"`
	SignedUpAt time.Time `json:"signed_up_at" avro:"signed_up_at"`
}

func main() {
	inMemBus := shmemory.NewBus(0)
	hub := streamhub.NewHub(
		streamhub.WithPublisher(shmemory.NewPublisher(inMemBus)),
		streamhub.WithListenerDriver(shmemory.NewListener(inMemBus)))

	hub.RegisterStream(studentSignedUp{}, streamhub.StreamMetadata{
		Stream: "student-signed_up",
	})

	_ = hub.Listen(studentSignedUp{},
		streamhub.WithGroup("example-job-on-student-signed_up"),
		streamhub.WithListenerFunc(func(ctx context.Context, message streamhub.Message) error {
			log.Printf("message decoded at reflection-based: %+v", message.DecodedData)
			log.Printf("consumed message from group: %s", message.GroupName)
			_, ok := message.DecodedData.(studentSignedUp)
			if !ok {
				log.Print("failed to cast reflection-message")
			}
			return errors.New("failed processing for reflection-based")
		}))
	hub.ListenByStreamKey("student-signed_up",
		streamhub.WithConcurrencyLevel(3),
		streamhub.WithGroup("second_example-job-on-student-signed_up"),
		streamhub.WithListenerFunc(func(ctx context.Context, message streamhub.Message) error {
			log.Printf("message decoded at string-based: %+v", message.DecodedData)
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

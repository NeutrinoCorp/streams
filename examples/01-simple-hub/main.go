package main

import (
	"context"
	"errors"
	"log"
	"time"

	"github.com/neutrinocorp/streamhub"
	"github.com/neutrinocorp/streamhub/driver/shmemory"
)

type studentSignedUp struct {
	StudentID  string    `json:"student_id" avro:"student_id"`
	SignedUpAt time.Time `json:"signed_up_at" avro:"signed_up_at"`
}

func main() {
	inMemBus := shmemory.NewBus(0)
	hub := streamhub.NewHub(
		streamhub.WithWriter(shmemory.NewWriter(inMemBus)),
		streamhub.WithReader(shmemory.NewReader(inMemBus)))

	hub.RegisterStream(studentSignedUp{}, streamhub.StreamMetadata{
		Stream: "student-signed_up",
	})

	_ = hub.Read(studentSignedUp{},
		streamhub.WithGroup("example-job-on-student-signed_up"),
		streamhub.WithHandlerFunc(func(ctx context.Context, message streamhub.Message) error {
			log.Printf("message decoded at reflection-based: %+v", message.DecodedData)
			log.Printf("consumed message from group: %s", message.GroupName)
			_, ok := message.DecodedData.(studentSignedUp)
			if !ok {
				log.Print("failed to cast reflection-message")
			}
			return errors.New("failed processing for reflection-based")
		}))
	hub.ReadByStreamKey("student-signed_up",
		streamhub.WithConcurrencyLevel(3),
		streamhub.WithGroup("second_example-job-on-student-signed_up"),
		streamhub.WithHandlerFunc(func(ctx context.Context, message streamhub.Message) error {
			log.Printf("message decoded at string-based: %+v", message.DecodedData)
			return nil
		}))

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*30)
	defer cancel()
	hub.Start(ctx)

	err := hub.Write(context.Background(), studentSignedUp{
		StudentID:  "2",
		SignedUpAt: time.Now().UTC(),
	})
	if err != nil {
		panic(err)
	}
	time.Sleep(time.Second * 10)
}

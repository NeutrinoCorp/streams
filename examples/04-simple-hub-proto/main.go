package main

import (
	"context"
	"log"
	"time"

	"github.com/neutrinocorp/streamhub/testdata/proto/github.com/neutrinocorp/examplepb"

	"github.com/neutrinocorp/streamhub"
	shmemory "github.com/neutrinocorp/streamhub/streamhub-memory"
)

func main() {
	inMemBus := shmemory.NewBus(0)
	marshaler := streamhub.ProtocolBuffersMarshaler{}
	hub := streamhub.NewHub(
		streamhub.WithMarshaler(marshaler),
		streamhub.WithPublisher(shmemory.NewPublisher(inMemBus)),
		streamhub.WithListenerDriver(shmemory.NewListener(inMemBus)))

	// DO NOT USE pointers to register streams when using ProtoBuf
	hub.RegisterStream(examplepb.Person{}, streamhub.StreamMetadata{
		Stream: "student-signed_up",
	})

	_ = hub.Listen(examplepb.Person{},
		streamhub.WithGroup("example-job-on-student-signed_up"),
		streamhub.WithListenerFunc(func(ctx context.Context, message streamhub.Message) error {
			log.Printf("message decoded at reflection-based: %+v", message.Data)
			log.Printf("consumed message from group: %s", message.GroupName)
			msg, ok := message.DecodedData.(*examplepb.Person)
			if ok {
				log.Printf("%+v", msg)
				log.Print(message.Subject)
			}
			return nil
		}))

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*30)
	defer cancel()
	hub.Start(ctx)

	err := hub.Publish(context.Background(), &examplepb.Person{
		Name:        "Alonso Ruiz",
		Id:          15,
		Email:       "aruiz@example.com",
		Phones:      nil,
		LastUpdated: nil,
	})
	if err != nil {
		panic(err)
	}
	time.Sleep(time.Second * 10)
}

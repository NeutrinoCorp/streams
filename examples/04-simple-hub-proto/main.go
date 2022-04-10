package main

import (
	"context"
	"log"
	"time"

	"github.com/neutrinocorp/streams"
	"github.com/neutrinocorp/streams/driver/shmemory"
	"github.com/neutrinocorp/streams/testdata/proto/examplepb"
)

func main() {
	inMemBus := shmemory.NewBus(0)
	marshaler := streams.ProtocolBuffersMarshaler{}
	hub := streams.NewHub(
		streams.WithMarshaler(marshaler),
		streams.WithWriter(shmemory.NewWriter(inMemBus)),
		streams.WithReader(shmemory.NewReader(inMemBus)))

	// DO NOT USE pointers to register streams when using ProtoBuf
	hub.RegisterStream(examplepb.Person{}, streams.StreamMetadata{
		Stream: "student-signed_up",
	})

	_ = hub.Read(examplepb.Person{},
		streams.WithGroup("example-job-on-student-signed_up"),
		streams.WithHandlerFunc(func(ctx context.Context, message streams.Message) error {
			log.Printf("message decoded at reflection-based: %+v", message.Data)
			log.Printf("consumed message from group: %s", message.GroupName)
			msg, ok := message.DecodedData.(*examplepb.Person)
			if ok {
				log.Printf("%+v", msg)
			}
			return nil
		}))

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*30)
	defer cancel()
	hub.Start(ctx)

	err := hub.Write(context.Background(), &examplepb.Person{
		Name:  "Alonso Ruiz",
		Id:    15,
		Email: "aruiz@example.com",
	})
	if err != nil {
		panic(err)
	}
	time.Sleep(time.Second * 3)
}

package main

import (
	"context"
	"log"
	"time"

	"github.com/neutrinocorp/streams"
	"github.com/neutrinocorp/streams/driver/shmemory"
)

type studentSignedUp struct {
	StudentID  string    `json:"student_id" avro:"student_id"`
	SignedUpAt time.Time `json:"signed_up_at" avro:"signed_up_at"`
}

func main() {
	inMemBus := shmemory.NewBus(0)
	hub := streams.NewHub(
		streams.WithWriter(shmemory.NewWriter(inMemBus)),
		streams.WithReader(shmemory.NewReader(inMemBus)),
		streams.WithSchemaRegistry(setupSchemaRegistry()),
		streams.WithMarshaler(streams.NewAvroMarshaler()))
	hub.RegisterStream(studentSignedUp{}, streams.StreamMetadata{
		Stream:               "student-signed_up",
		SchemaDefinitionName: "student-signed_up",
		SchemaVersion:        1,
	})

	_ = hub.Read(studentSignedUp{},
		streams.WithGroup("example-job-on-student-signed_up"),
		streams.WithHandlerFunc(func(ctx context.Context, message streams.Message) error {
			log.Printf("message decoded at reflection-based: %+v", message.DecodedData)
			return nil
		}))

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*30)
	defer cancel()
	hub.Start(ctx)

	err := hub.Write(context.Background(), studentSignedUp{
		StudentID:  "1",
		SignedUpAt: time.Now().UTC(),
	})
	if err != nil {
		panic(err)
	}
	time.Sleep(time.Second * 10)
}

func setupSchemaRegistry() streams.InMemorySchemaRegistry {
	r := streams.InMemorySchemaRegistry{}
	r.RegisterDefinition("student-signed_up", `{
		"type": "record",
		"name": "fooMessage",
		"namespace": "org.ncorp.avro",
		"fields" : [
			{"name": "student_id", "type": "string"},
			{"name": "signed_up_at", "type": "string"}
		]
	}`, 1)
	return r
}

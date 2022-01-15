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
	hub := streamhub.NewHub(
		streamhub.WithSchemaRegistry(setupSchemaRegistry()),
		streamhub.WithMarshaler(streamhub.NewAvroMarshaler()))
	hub.StreamRegistry.Set(studentSignedUp{}, streamhub.StreamMetadata{
		Stream:           "student-signed_up",
		SchemaDefinition: "student-signed_up",
		SchemaVersion:    1,
	})

	err := hub.Publish(context.Background(), studentSignedUp{
		StudentID:  "1",
		SignedUpAt: time.Now().UTC(),
	})
	if err != nil {
		panic(err)
	}
}

func setupSchemaRegistry() streamhub.InMemorySchemaRegistry {
	r := streamhub.InMemorySchemaRegistry{}
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

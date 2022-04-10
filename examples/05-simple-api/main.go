package main

import (
	"context"
	"errors"
	"log"
	"time"

	"github.com/neutrinocorp/streams"
	"github.com/neutrinocorp/streams/driver/shmemory"
)

type VisitEvent struct {
	ResourceID string `json:"resource_id"`
}

func main() {
	bus := shmemory.NewBus(100)
	streams.DefaultHub = streams.NewHub(
		streams.WithWriter(shmemory.NewWriter(bus)),
		streams.WithReader(shmemory.NewReader(bus)))

	// 1. Bind struct(s) to metadata (stream/topic name, schema name and/or version, ...)
	streams.RegisterStream(VisitEvent{}, streams.StreamMetadata{
		Stream: "org.ncorp.analytics.visit",
	})

	// 2. Register reading node(s) and its handler
	_ = streams.Read(VisitEvent{}, streams.WithHandlerFunc(func(ctx context.Context, message streams.Message) error {
		event, ok := message.DecodedData.(VisitEvent)
		if !ok {
			log.Print("invalid event format")
			return errors.New("invalid event format")
		}
		log.Printf("detected new visit to resource %s", event.ResourceID)
		return nil
	}))

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
	defer cancel()
	// 3. Start background jobs (e.g. reading nodes)
	streams.Start(ctx)

	// 4. Produce messages to streams
	err := streams.Write(context.Background(), VisitEvent{
		ResourceID: "abc123",
	})
	if err != nil {
		panic(err)
	}
	time.Sleep(time.Second * 3)
}

package main

import (
	"context"
	"log"
	"math/rand"
	"time"

	"github.com/neutrinocorp/streamhub"
	"github.com/neutrinocorp/streamhub/driver/shmemory"
)

type studentSignedUp struct {
	StudentID  string    `json:"student_id" avro:"student_id"`
	SignedUpAt time.Time `json:"signed_up_at" avro:"signed_up_at"`
}

type studentLoggedIn struct {
	StudentID  string    `json:"student_id"`
	Username   string    `json:"username"`
	LoggedInAt time.Time `json:"logged_in_at"`
}

type aggregateMetricOnStudent struct {
	Gauge int `json:"gauge"`
}

type doSomethingOnAggregate struct {
	Foo string `json:"foo"`
}

var loggingListener streamhub.ReaderBehaviour = func(node *streamhub.ReaderNode, hub *streamhub.Hub,
	next streamhub.ReaderHandleFunc) streamhub.ReaderHandleFunc {
	log.Printf("[RECEIVED] %s | host: %s", node.Stream, hub.InstanceName)
	log.Printf("[RECEIVED] %s | group: %s", node.Stream, node.Group)
	return func(ctx context.Context, message streamhub.Message) error {
		log.Printf("[RECEIVED] %s | group: %s | message_id: %s", node.Stream, node.Group, message.ID)
		log.Printf("[RECEIVED] %s | group: %s | correlation_id: %s", node.Stream, node.Group, message.CorrelationID)
		log.Printf("[RECEIVED] %s | group: %s | causation_id: %s", node.Stream, node.Group, message.CausationID)
		return next(ctx, message)
	}
}

func main() {
	inMemBus := shmemory.NewBus(0)
	hub := streamhub.NewHub(
		streamhub.WithIDFactory(streamhub.RandInt64Factory),
		streamhub.WithReaderBehaviours(loggingListener),
		streamhub.WithWriter(shmemory.NewWriter(inMemBus)),
		streamhub.WithReader(shmemory.NewReader(inMemBus)))

	hub.RegisterStream(studentSignedUp{}, streamhub.StreamMetadata{
		Stream: "student-signed_up",
	})

	hub.RegisterStream(studentLoggedIn{}, streamhub.StreamMetadata{
		Stream:        "student-logged_in",
		SchemaVersion: 8,
	})

	hub.RegisterStream(aggregateMetricOnStudent{}, streamhub.StreamMetadata{
		Stream: "aggregate_metric-on-student-logged_in",
	})

	hub.RegisterStream(doSomethingOnAggregate{}, streamhub.StreamMetadata{
		Stream: "do_something-on-aggregate",
	})

	_ = hub.Read(studentSignedUp{},
		streamhub.WithGroup("example-job-on-student-signed_up"),
		streamhub.WithHandlerFunc(func(ctx context.Context, message streamhub.Message) error {
			data, ok := message.DecodedData.(studentSignedUp)
			if !ok {
				log.Print("failed to cast reflection-message")
			}
			return hub.Write(ctx, studentLoggedIn{
				StudentID:  data.StudentID,
				Username:   "aruiz",
				LoggedInAt: time.Now().UTC(),
			})
		}))

	_ = hub.Read(studentLoggedIn{},
		streamhub.WithGroup("job-on-student-logged_in"),
		streamhub.WithHandlerFunc(func(ctx context.Context, message streamhub.Message) error {
			data, ok := message.DecodedData.(studentLoggedIn)
			if !ok {
				log.Print("failed to cast logged in reflection-message")
			}
			log.Printf("message decoded at reflection-based: %+v", data)
			return hub.Write(ctx, aggregateMetricOnStudent{
				Gauge: rand.Int(),
			})
		}))

	_ = hub.Read(aggregateMetricOnStudent{},
		streamhub.WithGroup("aggregate-on-student-logged_in"),
		streamhub.WithHandlerFunc(func(ctx context.Context, message streamhub.Message) error {
			data, ok := message.DecodedData.(aggregateMetricOnStudent)
			if !ok {
				log.Print("failed")
			}
			log.Printf("gauge: %d", data.Gauge)
			return hub.Write(ctx, doSomethingOnAggregate{
				Foo: "bar",
			})
		}))

	_ = hub.Read(doSomethingOnAggregate{},
		streamhub.WithHandlerFunc(func(ctx context.Context, message streamhub.Message) error {
			return nil
		}))

	_ = hub.Read(doSomethingOnAggregate{},
		streamhub.WithGroup("foo-group"),
		streamhub.WithHandlerFunc(func(ctx context.Context, message streamhub.Message) error {
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

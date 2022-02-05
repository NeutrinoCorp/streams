package main

import (
	"context"
	"log"
	"sync/atomic"
	"time"

	"github.com/neutrinocorp/streamhub"
	"github.com/neutrinocorp/streamhub/shmemory"
)

type transactionRegistered struct {
	TxID   string  `json:"tx_id" avro:"tx_id"`
	Amount float64 `json:"amount" avro:"amount"`
}

var totalProcessedMessages = uint64(0)

func main() {
	baseCtx, cancel := context.WithCancel(context.Background())
	defer cancel()

	b := shmemory.NewBus()
	hub := streamhub.NewHub(
		streamhub.WithSchemaRegistry(newAvroSchemaRegistry()),
		streamhub.WithMarshaler(streamhub.NewAvroMarshaler()),
		streamhub.WithPublisher(shmemory.NewPublisher(b)),
		streamhub.WithListenerDriver(shmemory.NewListener(b)))

	registerStream(hub)
	registerListeners(hub)

	go func() {
		hub.Start(baseCtx)
	}()

	timeFrame := time.NewTimer(time.Second * 1)
	defer timeFrame.Stop()
	publishMessages(timeFrame, hub)
	time.Sleep(time.Second * 1)
	log.Printf("processed messages: %d msg/sec", totalProcessedMessages)
}

func registerStream(h *streamhub.Hub) {
	h.RegisterStream(transactionRegistered{}, streamhub.StreamMetadata{
		Stream:               "ncorp.wallet.tx.registered",
		SchemaDefinitionName: "wallet-tx-registered",
		SchemaVersion:        1,
		GoType:               nil,
	})
}

func newAvroSchemaRegistry() streamhub.SchemaRegistry {
	registry := streamhub.InMemorySchemaRegistry{}
	registry.RegisterDefinition("wallet-tx-registered", `{
		"type": "record",
		"name": "transactionRegistered",
		"namespace": "org.ncorp.wallet",
		"fields" : [
			{"name": "tx_id", "type": "string"},
			{"name": "amount", "type": "double"}
		]
	}`, 1)
	return registry
}

func registerListeners(h *streamhub.Hub) {
	_ = h.Listen(transactionRegistered{},
		streamhub.WithListenerFunc(func(ctx context.Context, message streamhub.Message) error {
			atomic.AddUint64(&totalProcessedMessages, 1)
			return nil
		}))
}

func publishMessages(timeFrame *time.Timer, h *streamhub.Hub) {
	for {
		go func() {
			err := h.Publish(context.Background(), transactionRegistered{
				TxID:   "1",
				Amount: 99.99,
			})
			if err != nil {
				log.Print(err)
			}
		}()
		select {
		case <-timeFrame.C:
			return
		default:
		}
	}
}

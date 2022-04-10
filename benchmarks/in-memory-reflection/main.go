package main

import (
	"context"
	"log"
	"runtime"
	"sync/atomic"
	"time"

	"github.com/neutrinocorp/streams"
	"github.com/neutrinocorp/streams/driver/shmemory"
)

type transactionRegistered struct {
	TxID   string  `json:"tx_id"`
	Amount float64 `json:"amount"`
}

var totalProcessedMessages = uint64(0)

func main() {
	memStats := &runtime.MemStats{}
	defer func() {
		runtime.ReadMemStats(memStats)
		log.Printf("total memory allocation: %d", memStats.TotalAlloc)
		log.Printf("memory allocation: %d", memStats.Mallocs)
		log.Printf("heap allocation: %d", memStats.HeapAlloc)
		log.Printf("heap allocation in use: %d", memStats.HeapInuse)
		log.Printf("heap allocation freed: %d", memStats.Frees)
		log.Printf("heap allocation released: %d", memStats.HeapReleased)
	}()
	baseCtx, cancel := context.WithCancel(context.Background())
	defer cancel()

	b := shmemory.NewBus(0)
	hub := streams.NewHub(
		streams.WithWriter(shmemory.NewWriter(b)),
		streams.WithReader(shmemory.NewReader(b)))

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

func registerStream(h *streams.Hub) {
	h.RegisterStream(transactionRegistered{}, streams.StreamMetadata{
		Stream: "ncorp.wallet.tx.registered",
	})
}

func registerListeners(h *streams.Hub) {
	_ = h.Read(transactionRegistered{},
		streams.WithHandlerFunc(func(ctx context.Context, message streams.Message) error {
			atomic.AddUint64(&totalProcessedMessages, 1)
			return nil
		}))
}

func publishMessages(timeFrame *time.Timer, h *streams.Hub) {
	for {
		go func() {
			err := h.Write(context.Background(), transactionRegistered{
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

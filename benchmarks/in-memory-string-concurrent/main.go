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
var totalGoroutines = uint64(0)

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

	b := shmemory.NewBus(1024)
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
	time.Sleep(time.Second)
	log.Printf("processed messages: %d msg/sec", totalProcessedMessages)
	log.Printf("total goroutines used: %d", totalGoroutines)
	//time.Sleep(time.Second * 3)
	log.Printf("total goroutines at shutdown: %d", runtime.NumGoroutine())
}

func registerStream(h *streams.Hub) {
	h.RegisterStreamByString("ncorp.wallet.tx.registered", streams.StreamMetadata{
		Stream: "ncorp.wallet.tx.registered",
		GoType: nil,
	})
}

func registerListeners(h *streams.Hub) {
	h.ReadByStreamKey("ncorp.wallet.tx.registered",
		streams.WithConcurrencyLevel(2),
		streams.WithHandlerFunc(func(ctx context.Context, message streams.Message) error {
			atomic.AddUint64(&totalProcessedMessages, 1)
			atomic.StoreUint64(&totalGoroutines, uint64(runtime.NumGoroutine()))
			return nil
		}))
}

func publishMessages(timeFrame *time.Timer, h *streams.Hub) {
	for {
		go func() {
			_ = h.WriteByMessageKey(context.Background(), "ncorp.wallet.tx.registered", transactionRegistered{
				TxID:   "1",
				Amount: 99.99,
			})
		}()
		select {
		case <-timeFrame.C:
			return
		default:
		}
	}
}

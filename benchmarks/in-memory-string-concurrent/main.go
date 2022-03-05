package main

import (
	"context"
	"log"
	"runtime"
	"sync/atomic"
	"time"

	"github.com/neutrinocorp/streamhub"
	shmemory "github.com/neutrinocorp/streamhub/streamhub-memory"
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
	hub := streamhub.NewHub(
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
	time.Sleep(time.Second)
	log.Printf("processed messages: %d msg/sec", totalProcessedMessages)
	log.Printf("total goroutines used: %d", totalGoroutines)
	//time.Sleep(time.Second * 3)
	log.Printf("total goroutines at shutdown: %d", runtime.NumGoroutine())
}

func registerStream(h *streamhub.Hub) {
	h.RegisterStreamByString("ncorp.wallet.tx.registered", streamhub.StreamMetadata{
		Stream: "ncorp.wallet.tx.registered",
		GoType: nil,
	})
}

func registerListeners(h *streamhub.Hub) {
	h.ListenByStreamKey("ncorp.wallet.tx.registered",
		streamhub.WithConcurrencyLevel(2),
		streamhub.WithListenerFunc(func(ctx context.Context, message streamhub.Message) error {
			atomic.AddUint64(&totalProcessedMessages, 1)
			atomic.StoreUint64(&totalGoroutines, uint64(runtime.NumGoroutine()))
			return nil
		}))
}

func publishMessages(timeFrame *time.Timer, h *streamhub.Hub) {
	for {
		go func() {
			_ = h.PublishByMessageKey(context.Background(), "ncorp.wallet.tx.registered", transactionRegistered{
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

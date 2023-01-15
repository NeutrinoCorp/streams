package kafka

import (
	"context"
	"sync"
	"sync/atomic"

	"github.com/Shopify/sarama"
	"github.com/neutrinocorp/streams/v2"
)

type SyncWriter struct {
	client sarama.SyncProducer
}

var _ streams.Writer = SyncWriter{}

func NewSyncWriter(c sarama.SyncProducer) SyncWriter {
	return SyncWriter{client: c}
}

func (k SyncWriter) Write(ctx context.Context, p streams.Message) (int, error) {
	return k.WriteMany(ctx, []streams.Message{p})
}

func (k SyncWriter) WriteMany(_ context.Context, p []streams.Message) (n int, err error) {
	msgs := make([]*sarama.ProducerMessage, 0, len(p))
	for _, msg := range p {
		kMsg := newKMessage(msg)
		msgs = append(msgs, &kMsg)
		n += kMsg.Value.Length()
	}
	err = k.client.SendMessages(msgs)
	return
}

func (k SyncWriter) Close() error {
	return k.client.Close()
}

type AsyncWriter struct {
	client sarama.AsyncProducer
}

var _ streams.Writer = AsyncWriter{}

func NewAsyncWriter(p sarama.AsyncProducer) AsyncWriter {
	return AsyncWriter{
		client: p,
	}
}

func (k AsyncWriter) Write(ctx context.Context, p streams.Message) (n int, err error) {
	return k.WriteMany(ctx, []streams.Message{p})
}

func (k AsyncWriter) WriteMany(_ context.Context, p []streams.Message) (n int, err error) {
	wg := sync.WaitGroup{}
	wg.Add(len(p))
	var bytesOut uintptr = 0
	go func() {
		for err = range k.client.Errors() {
			wg.Done()
		}
	}()
	go func() {
		for m := range k.client.Successes() {
			atomic.AddUintptr(&bytesOut, uintptr(m.Value.Length()))
			wg.Done()
		}
	}()

	for _, msg := range p {
		kMsg := newKMessage(msg)
		k.client.Input() <- &kMsg
	}

	wg.Wait()
	n = int(bytesOut)
	return
}

func (k AsyncWriter) Close() error {
	return k.client.Close()
}

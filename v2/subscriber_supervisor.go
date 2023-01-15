package streams

import (
	"context"
	"log"
	"sync"
	"time"

	"golang.org/x/sync/semaphore"
)

type subscriberTask struct {
	metadata StreamMetadata
	handler  SubscriberFunc
	timeout  time.Duration
	reader   Reader
}

type SubscriberSupervisor struct {
	reg            *StreamRegistry
	reader         Reader
	codec          Codec
	subscribers    []subscriberTask
	handlerTimeout time.Duration
	baseCtx        context.Context
	cancelBaseCtx  context.CancelFunc
	sem            *semaphore.Weighted
}

func NewSubscriberSupervisor(reg *StreamRegistry, r Reader) *SubscriberSupervisor {
	handlerTimeout := time.Second * 3
	return &SubscriberSupervisor{
		reg:            reg,
		reader:         r,
		codec:          JSONCodec{},
		subscribers:    make([]subscriberTask, 0),
		handlerTimeout: handlerTimeout,
		baseCtx:        nil,
		sem:            nil,
	}
}

func (s *SubscriberSupervisor) RegisterSubscriber(message interface{}, subscriberFunc SubscriberFunc,
	opts ...SubscriberOption) {
	metadata, err := s.reg.GetByType(message)
	if err != nil {
		return
	}

	sub := subscriberTask{
		metadata: metadata,
		handler:  subscriberFunc,
		timeout:  time.Second * 3,
		reader:   s.reader,
	}

	for _, o := range opts {
		o.apply(&sub)
	}

	s.subscribers = append(s.subscribers, sub)
}

func (s *SubscriberSupervisor) Start() {
	s.baseCtx, s.cancelBaseCtx = context.WithCancel(context.Background())
	wg := sync.WaitGroup{}
	wg.Add(len(s.subscribers))
	s.sem = semaphore.NewWeighted(int64(len(s.subscribers)))
	for _, entry := range s.subscribers {
		if errSem := s.sem.Acquire(s.baseCtx, 1); errSem != nil {
			log.Print(errSem)
			continue
		}
		go func(sub subscriberTask) {
			wg.Done()
			defer s.sem.Release(1)
			_ = sub.reader.Read(s.baseCtx, sub.metadata.Name, func(ctx context.Context, msg Message) error {
				valRef := sub.metadata.GoType.New()
				if err := s.codec.Unmarshal(msg.Data, valRef); err != nil {
					return err
				}

				msg.DecodedData = sub.metadata.GoType.Indirect(valRef) // DO NOT deref for protobuf
				scopedCtx, cancel := context.WithTimeout(s.baseCtx, sub.timeout)
				defer cancel()
				if err := sub.handler(scopedCtx, msg); err != nil { // root sub func exec
					return err
				}
				return nil
			})

		}(entry)
	}
	wg.Wait()
}

func (s *SubscriberSupervisor) Shutdown() {
	log.Print("closing subscriber supervisor")
	s.cancelBaseCtx()
	wg := sync.WaitGroup{}
	wg.Add(len(s.subscribers))
	for _, entry := range s.subscribers {
		go func(sub subscriberTask) {
			_ = sub.reader.Close()
			wg.Done()
		}(entry)
	}
	wg.Wait()
}

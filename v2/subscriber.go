package streams

import (
	"context"
	"log"
	"sync"
	"time"
)

type SubscriberFunc func(ctx context.Context, msg Message) error

type subscriberTask struct {
	Metadata StreamMetadata
	Handler  SubscriberFunc
}

type SubscriberSupervisor struct {
	reg            *StreamRegistry
	reader         Reader
	codec          Codec
	subscribers    []subscriberTask
	handlerTimeout time.Duration
	baseCtx        context.Context
	cancelBaseCtx  context.CancelFunc
}

func NewSubscriberSupervisor(reg *StreamRegistry, r Reader) *SubscriberSupervisor {
	return &SubscriberSupervisor{
		reg:            reg,
		reader:         r,
		codec:          JSONCodec{},
		subscribers:    make([]subscriberTask, 0),
		handlerTimeout: time.Second * 3,
		baseCtx:        nil,
	}
}

func (s *SubscriberSupervisor) Add(message interface{}, subscriberFunc SubscriberFunc) {
	metadata, err := s.reg.GetByType(message)
	if err != nil {
		return
	}
	s.subscribers = append(s.subscribers, subscriberTask{
		Metadata: metadata,
		Handler:  subscriberFunc,
	})
}

func (s *SubscriberSupervisor) Start() {
	s.baseCtx, s.cancelBaseCtx = context.WithCancel(context.Background())
	wg := sync.WaitGroup{}
	wg.Add(len(s.subscribers))
	for _, entry := range s.subscribers {
		go func(sub subscriberTask) {
			wg.Done()
			_ = s.reader.Read(s.baseCtx, sub.Metadata.Name, func(ctx context.Context, msg Message) error {
				valRef := sub.Metadata.GoType.New()
				if err := s.codec.Unmarshal(msg.Data, valRef); err != nil {
					return err
				}

				msg.DecodedData = sub.Metadata.GoType.Indirect(valRef) // DO NOT deref for protobuf
				scopedCtx, cancel := context.WithTimeout(s.baseCtx, s.handlerTimeout)
				defer cancel()
				if err := sub.Handler(scopedCtx, msg); err != nil { // root sub func exec
					return err
				}
				return nil
			})

		}(entry)
	}
	wg.Wait()
}

func (s *SubscriberSupervisor) Shutdown() {
	s.cancelBaseCtx()
	log.Print("closing subscriber supervisor")
}

package streams

import (
	"context"
	"time"
)

type Publisher struct {
	writer        Writer
	codec         Codec
	idFactoryFunc IdentifierFactoryFunc
	reg           *StreamRegistry
}

func newDefaultPublisherOpts() publisherOpts {
	return publisherOpts{
		codec:     JSONCodec{},
		idFactory: GoogleUUIDFactory,
	}
}

func NewPublisher(opts ...PublisherOption) Publisher {
	baseCfg := newDefaultPublisherOpts()
	for _, o := range opts {
		o.applyPublisher(&baseCfg)
	}
	return Publisher{
		writer:        baseCfg.writer,
		codec:         baseCfg.codec,
		idFactoryFunc: baseCfg.idFactory,
		reg:           baseCfg.reg,
	}
}

func (p Publisher) newMessageFromPublish(msg interface{}, headers Headers) (Message, error) {
	metadata, err := p.reg.GetByType(msg)
	if err != nil {
		return Message{}, err
	}

	id, err := p.idFactoryFunc()
	if err != nil {
		return Message{}, err
	}

	data, err := p.codec.Marshal(msg)
	if err != nil {
		return Message{}, err
	}

	return Message{
		ID:              id,
		CorrelationID:   id,
		CausationID:     id,
		StreamName:      metadata.Name,
		ContentType:     p.codec.GetContentType(),
		SchemaURL:       metadata.SchemaURL,
		TimestampMillis: time.Now().UTC().UnixMilli(),
		Data:            data,
		Headers:         headers,
	}, nil
}

type PublishMessageArgs struct {
	Data    interface{}
	Headers Headers
}

func (p Publisher) Publish(ctx context.Context, msgs ...PublishMessageArgs) (int, error) {
	return p.PublishTo(p.writer, ctx, msgs...)
}

func (p Publisher) PublishTo(w Writer, ctx context.Context, msgs ...PublishMessageArgs) (int, error) {
	if len(msgs) == 1 {
		msg, err := p.newMessageFromPublish(msgs[0].Data, msgs[0].Headers)
		if err != nil {
			return 0, err
		}
		return w.Write(ctx, msg)
	}

	msgBuf := make([]Message, 0, len(msgs))
	for _, msgPub := range msgs {
		msg, err := p.newMessageFromPublish(msgPub.Data, msgPub.Headers)
		if err != nil {
			return 0, err
		}
		msgBuf = append(msgBuf, msg)
	}
	return w.WriteMany(ctx, msgBuf)
}

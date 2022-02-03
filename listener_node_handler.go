package streamhub

import (
	"context"
	"reflect"

	"github.com/cenkalti/backoff/v4"
)

// ListenerNodeHandler is the handling function for each incoming message from a stream used by streamhub's
// ListenerDriver(s).
//
// The base handler contains retry backoff mechanisms, correlation and context id injection,
type ListenerNodeHandler func(context.Context, Message) error

func listenerNodeHandlerRetryBackoff(baseOpts listenerNodeOptions, baseHandler ListenerNodeHandler) ListenerNodeHandler {
	b := backoff.NewExponentialBackOff()
	b.InitialInterval = baseOpts.retryInitialInterval
	b.MaxInterval = baseOpts.retryMaxInterval
	b.MaxElapsedTime = baseOpts.retryTimeout
	return func(ctx context.Context, message Message) error {
		return backoff.Retry(func() error {
			return baseHandler(ctx, message)
		}, b)
	}
}

func listenerNodeHandlerUnmarshaling(h *Hub, baseHandler ListenerNodeHandler) ListenerNodeHandler {
	return func(ctx context.Context, message Message) error {
		metadata, err := h.StreamRegistry.GetByStreamName(message.Stream)
		if err != nil {
			return err
		}
		var schemaDef string
		if h.SchemaRegistry != nil {
			schemaDef, err = h.SchemaRegistry.GetSchemaDefinition(metadata.SchemaDefinitionName,
				metadata.SchemaVersion)
			if err != nil {
				return err
			}
		}
		if metadata.GoType != nil {
			decodedData := reflect.New(metadata.GoType)
			if err = h.Marshaler.Unmarshal(schemaDef, message.Data, decodedData.Interface()); err != nil {
				return err
			}
			message.DecodedData = decodedData
		}
		return baseHandler(ctx, message)
	}
}

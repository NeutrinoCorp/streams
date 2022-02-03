package streamhub

import (
	"context"
	"reflect"

	"github.com/cenkalti/backoff/v4"
)

func retryListenerNodeBehaviour(baseOpts listenerNodeOptions, baseHandler ListenerNodeHandler) ListenerNodeHandler {
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

func unmarshalListenerNodeBehaviour(h *Hub, baseHandler ListenerNodeHandler) ListenerNodeHandler {
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

func injectGroupListenerNodeBehaviour(baseOpts listenerNodeOptions, baseHandler ListenerNodeHandler) ListenerNodeHandler {
	return func(ctx context.Context, message Message) error {
		message.GroupName = baseOpts.group
		return baseHandler(ctx, message)
	}
}

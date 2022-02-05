package streamhub

import (
	"context"
	"reflect"

	"github.com/cenkalti/backoff/v4"
)

// ListenerBehaviour is a middleware function with extra functionality which will be executed prior a ListenerFunc
// or Listener component for every stream-listening job instance registered into a Hub.
//
// The middleware gets injected the context ListenerNode (the stream-listening job to be executed), the root Hub instance and
// the parent middleware function.
//
// Moreover, there are built-in behaviours ready to be used with streamhub:
//
// - Retry backoff
//
// - Correlation and causation ID injection
//
// - Consumer group injection
//
// - Auto-unmarshalling (*only if using reflection-based stream registry or GoType was defined when registering stream)
//
// - Logging*
//
// - Metrics*
//
// - Tracing*
//
// *Manual specification on configuration required
type ListenerBehaviour func(node *ListenerNode, hub *Hub, next ListenerFunc) ListenerFunc

// will execute with desc order
var listenerBaseBehaviours = []ListenerBehaviour{
	unmarshalListenerBehaviour,
	injectGroupListenerBehaviour,
	injectTxIDsListenerBehaviour,
	retryListenerBehaviour,
}

var retryListenerBehaviour ListenerBehaviour = func(node *ListenerNode, _ *Hub, next ListenerFunc) ListenerFunc {
	b := backoff.NewExponentialBackOff()
	b.InitialInterval = node.RetryInitialInterval
	b.MaxInterval = node.RetryMaxInterval
	b.MaxElapsedTime = node.RetryTimeout
	return func(ctx context.Context, message Message) error {
		return backoff.Retry(func() error {
			return next(ctx, message)
		}, b)
	}
}

var unmarshalListenerBehaviour ListenerBehaviour = func(_ *ListenerNode, h *Hub, next ListenerFunc) ListenerFunc {
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
			message.DecodedData = decodedData.Elem().Interface()
		}
		return next(ctx, message)
	}
}

var injectGroupListenerBehaviour ListenerBehaviour = func(node *ListenerNode, _ *Hub, next ListenerFunc) ListenerFunc {
	return func(ctx context.Context, message Message) error {
		message.GroupName = node.Group
		return next(ctx, message)
	}
}

var injectTxIDsListenerBehaviour ListenerBehaviour = func(_ *ListenerNode, h *Hub, next ListenerFunc) ListenerFunc {
	return func(ctx context.Context, message Message) error {
		ctxCorrelation := context.WithValue(ctx, ContextCorrelationID, MessageContextKey(message.CorrelationID))
		ctxCausation := context.WithValue(ctxCorrelation, ContextCausationID, MessageContextKey(message.ID))
		return next(ctxCausation, message)
	}
}

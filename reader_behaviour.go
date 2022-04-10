package streamhub

import (
	"context"

	"github.com/cenkalti/backoff/v4"
)

// ReaderBehaviour is a middleware function with extra functionality which will be executed prior a ReaderHandleFunc
// or Reader component for every stream-reading job instance registered into a Hub.
//
// The middleware gets injected the context ReaderNode (the stream-reading job to be executed), the root Hub instance and
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
type ReaderBehaviour func(node *ReaderNode, hub *Hub, next ReaderHandleFunc) ReaderHandleFunc

// ReaderBaseBehaviours default ReaderBehaviours
//
// Behaviours will be executed in descending order
var ReaderBaseBehaviours = []ReaderBehaviour{
	unmarshalReaderBehaviour,
	injectGroupReaderBehaviour,
	injectTxIDsReaderBehaviour,
	retryReaderBehaviour,
}

// ReaderBaseBehavioursNoUnmarshal default ReaderBehaviours without unmarshaling
//
// Behaviours will be executed in descending order
var ReaderBaseBehavioursNoUnmarshal = []ReaderBehaviour{
	injectGroupReaderBehaviour,
	injectTxIDsReaderBehaviour,
	retryReaderBehaviour,
}

var retryReaderBehaviour ReaderBehaviour = func(node *ReaderNode, _ *Hub, next ReaderHandleFunc) ReaderHandleFunc {
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

var unmarshalReaderBehaviour ReaderBehaviour = func(_ *ReaderNode, h *Hub, next ReaderHandleFunc) ReaderHandleFunc {
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
			decodedData := metadata.GoType.New()
			if err = h.Marshaler.Unmarshal(schemaDef, message.Data, decodedData); err != nil {
				return err
			}
			message.DecodedData = decodedData
		}
		return next(ctx, message)
	}
}

var injectGroupReaderBehaviour ReaderBehaviour = func(node *ReaderNode, _ *Hub, next ReaderHandleFunc) ReaderHandleFunc {
	return func(ctx context.Context, message Message) error {
		message.GroupName = node.Group
		return next(ctx, message)
	}
}

var injectTxIDsReaderBehaviour ReaderBehaviour = func(_ *ReaderNode, h *Hub, next ReaderHandleFunc) ReaderHandleFunc {
	return func(ctx context.Context, message Message) error {
		ctxCorrelation := context.WithValue(ctx, ContextCorrelationID, MessageContextKey(message.CorrelationID))
		ctxCausation := context.WithValue(ctxCorrelation, ContextCausationID, MessageContextKey(message.ID))
		return next(ctxCausation, message)
	}
}

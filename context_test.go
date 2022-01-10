package streamhub_test

import (
	"context"
	"testing"

	"github.com/neutrinocorp/streamhub"
	"github.com/stretchr/testify/assert"
)

var injectCorrelationSuite = []struct {
	InCtx context.Context
	In    string
	Exp   string
}{
	{
		InCtx: nil,
		In:    "",
		Exp:   "",
	},
	{
		InCtx: nil,
		In:    "123",
		Exp:   "123",
	},
	{
		InCtx: context.WithValue(context.Background(), streamhub.ContextCorrelationID, "abc"),
		In:    "123",
		Exp:   "123",
	},
	{
		InCtx: context.WithValue(context.Background(), streamhub.ContextCausationID,
			streamhub.MessageContextKey("abc")),
		In:  "123",
		Exp: "123",
	},
	{
		InCtx: context.WithValue(context.Background(), streamhub.ContextCorrelationID,
			streamhub.MessageContextKey("abc")),
		In:  "123",
		Exp: "abc",
	},
}

func TestInjectMessageCorrelationID(t *testing.T) {
	for _, tt := range injectCorrelationSuite {
		id := streamhub.InjectMessageCorrelationID(tt.InCtx, tt.In)
		assert.Equal(t, tt.Exp, id)
	}
}

var injectCausationSuite = []struct {
	InCtx context.Context
	In    string
	Exp   string
}{
	{
		InCtx: nil,
		In:    "",
		Exp:   "",
	},
	{
		InCtx: nil,
		In:    "123",
		Exp:   "123",
	},
	{
		InCtx: context.WithValue(context.Background(), streamhub.ContextCausationID, "abc"),
		In:    "123",
		Exp:   "123",
	},
	{
		InCtx: context.WithValue(context.Background(), streamhub.ContextCorrelationID,
			streamhub.MessageContextKey("abc")),
		In:  "123",
		Exp: "123",
	},
	{
		InCtx: context.WithValue(context.Background(), streamhub.ContextCausationID,
			streamhub.MessageContextKey("abc")),
		In:  "123",
		Exp: "abc",
	},
}

func TestInjectMessageCausationID(t *testing.T) {
	for _, tt := range injectCausationSuite {
		id := streamhub.InjectMessageCausationID(tt.InCtx, tt.In)
		assert.Equal(t, tt.Exp, id)
	}
}

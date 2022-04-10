package streams_test

import (
	"context"
	"testing"

	"github.com/neutrinocorp/streams"
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
		InCtx: context.WithValue(context.Background(), streams.ContextCorrelationID, "abc"),
		In:    "123",
		Exp:   "123",
	},
	{
		InCtx: context.WithValue(context.Background(), streams.ContextCausationID,
			streams.MessageContextKey("abc")),
		In:  "123",
		Exp: "123",
	},
	{
		InCtx: context.WithValue(context.Background(), streams.ContextCorrelationID,
			streams.MessageContextKey("abc")),
		In:  "123",
		Exp: "abc",
	},
}

func TestInjectMessageCorrelationID(t *testing.T) {
	for _, tt := range injectCorrelationSuite {
		id := streams.InjectMessageCorrelationID(tt.InCtx, tt.In)
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
		InCtx: context.WithValue(context.Background(), streams.ContextCausationID, "abc"),
		In:    "123",
		Exp:   "123",
	},
	{
		InCtx: context.WithValue(context.Background(), streams.ContextCorrelationID,
			streams.MessageContextKey("abc")),
		In:  "123",
		Exp: "123",
	},
	{
		InCtx: context.WithValue(context.Background(), streams.ContextCausationID,
			streams.MessageContextKey("abc")),
		In:  "123",
		Exp: "abc",
	},
}

func TestInjectMessageCausationID(t *testing.T) {
	for _, tt := range injectCausationSuite {
		id := streams.InjectMessageCausationID(tt.InCtx, tt.In)
		assert.Equal(t, tt.Exp, id)
	}
}

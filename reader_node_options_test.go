package streams

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/stretchr/testify/assert"
)

func TestWithHandler(t *testing.T) {
	opt := WithHandler(ReaderHandlerNoop{})
	require.Implements(t, (*ReaderNodeOption)(nil), opt)

	hub := NewHub()
	hub.ReadByStreamKey("foo", opt)
	itemInterface, _ := hub.readerSupervisor.readerRegistry["foo"].Get(0)
	item := itemInterface.(ReaderNode)
	require.NotNil(t,
		item.HandlerFunc)

	hub.ReadByStreamKey("bar")
	itemInterface, _ = hub.readerSupervisor.readerRegistry["bar"].Get(0)
	item = itemInterface.(ReaderNode)
	assert.Nil(t,
		item.HandlerFunc)
}

func TestWithHandlerFunc(t *testing.T) {
	opt := WithHandlerFunc(func(_ context.Context, _ Message) error {
		return nil
	})
	require.Implements(t, (*ReaderNodeOption)(nil), opt)

	hub := NewHub()
	hub.ReadByStreamKey("foo", opt)

	itemInterface, _ := hub.readerSupervisor.readerRegistry["foo"].Get(0)
	item := itemInterface.(ReaderNode)
	require.NotNil(t, item.HandlerFunc)

	hub.ReadByStreamKey("bar")
	itemInterface, _ = hub.readerSupervisor.readerRegistry["bar"].Get(0)
	item = itemInterface.(ReaderNode)
	assert.Nil(t, item.HandlerFunc)
}

func TestWithGroup(t *testing.T) {
	opt := WithGroup("bar-job")
	require.Implements(t, (*ReaderNodeOption)(nil), opt)

	hub := NewHub()
	hub.ReadByStreamKey("foo", opt)
	itemInterface, _ := hub.readerSupervisor.readerRegistry["foo"].Get(0)
	item := itemInterface.(ReaderNode)
	assert.Equal(t, "bar-job", item.Group)
}

func TestWithConcurrencyLevel(t *testing.T) {
	opt := WithConcurrencyLevel(-1)
	require.Implements(t, (*ReaderNodeOption)(nil), opt)

	hub := NewHub()
	hub.ReadByStreamKey("foo", opt)
	itemInterface, _ := hub.readerSupervisor.readerRegistry["foo"].Get(0)
	item := itemInterface.(ReaderNode)
	require.Equal(t, 1, item.ConcurrencyLevel)

	opt = WithConcurrencyLevel(0)
	hub.ReadByStreamKey("bar", opt)
	itemInterface, _ = hub.readerSupervisor.readerRegistry["bar"].Get(0)
	item = itemInterface.(ReaderNode)
	require.Equal(t, 1, item.ConcurrencyLevel)

	opt = WithConcurrencyLevel(2)
	hub.ReadByStreamKey("bar", opt)
	itemInterface, _ = hub.readerSupervisor.readerRegistry["bar"].Get(1)
	item = itemInterface.(ReaderNode)
	assert.Equal(t, 2, item.ConcurrencyLevel)
}

func TestWithRetryInitialInterval(t *testing.T) {
	opt := WithRetryInitialInterval(time.Second * 5)
	require.Implements(t, (*ReaderNodeOption)(nil), opt)

	hub := NewHub()
	hub.ReadByStreamKey("foo", opt)
	itemInterface, _ := hub.readerSupervisor.readerRegistry["foo"].Get(0)
	item := itemInterface.(ReaderNode)
	assert.EqualValues(t, time.Second*5, item.RetryInitialInterval)
}

func TestWithRetryMaxInterval(t *testing.T) {
	opt := WithRetryMaxInterval(time.Second * 5)
	require.Implements(t, (*ReaderNodeOption)(nil), opt)

	hub := NewHub()
	hub.ReadByStreamKey("foo", opt)
	itemInterface, _ := hub.readerSupervisor.readerRegistry["foo"].Get(0)
	item := itemInterface.(ReaderNode)
	assert.EqualValues(t, time.Second*5, item.RetryMaxInterval)
}

func TestWithRetryTimeout(t *testing.T) {
	opt := WithRetryTimeout(time.Second * 5)
	require.Implements(t, (*ReaderNodeOption)(nil), opt)

	hub := NewHub()
	hub.ReadByStreamKey("foo", opt)
	itemInterface, _ := hub.readerSupervisor.readerRegistry["foo"].Get(0)
	item := itemInterface.(ReaderNode)
	assert.EqualValues(t, time.Second*5, item.RetryTimeout)
}

type fakeProviderCfg struct {
	Foo string
}

func TestWithProviderConfiguration(t *testing.T) {
	opt := WithProviderConfiguration(fakeProviderCfg{
		Foo: "foo",
	})
	require.Implements(t, (*ReaderNodeOption)(nil), opt)

	hub := NewHub()
	hub.ReadByStreamKey("foo", opt)
	itemInterface, _ := hub.readerSupervisor.readerRegistry["foo"].Get(0)
	item := itemInterface.(ReaderNode)
	assert.EqualValues(t, fakeProviderCfg{
		Foo: "foo",
	}, item.ProviderConfiguration)
}

func TestWithDriver(t *testing.T) {
	opt := WithDriver(readerNoop{})
	require.Implements(t, (*ReaderNodeOption)(nil), opt)

	hub := NewHub()
	hub.ReadByStreamKey("foo", opt)
	itemInterface, _ := hub.readerSupervisor.readerRegistry["foo"].Get(0)
	item := itemInterface.(ReaderNode)
	assert.EqualValues(t, readerNoop{},
		item.Reader)
}

func TestWithMaxHandlerPoolSize(t *testing.T) {
	opt := WithMaxHandlerPoolSize(-1)
	require.Implements(t, (*ReaderNodeOption)(nil), opt)

	hub := NewHub()
	hub.ReadByStreamKey("foo", opt)
	itemInterface, _ := hub.readerSupervisor.readerRegistry["foo"].Get(0)
	item := itemInterface.(ReaderNode)
	assert.Equal(t, DefaultMaxHandlerPoolSize, item.MaxHandlerPoolSize)

	opt = WithMaxHandlerPoolSize(0)
	hub.ReadByStreamKey("bar", opt)
	itemInterface, _ = hub.readerSupervisor.readerRegistry["bar"].Get(0)
	item = itemInterface.(ReaderNode)
	assert.Equal(t, DefaultMaxHandlerPoolSize, item.MaxHandlerPoolSize)

	opt = WithMaxHandlerPoolSize(2)
	hub.ReadByStreamKey("bar", opt)
	itemInterface, _ = hub.readerSupervisor.readerRegistry["bar"].Get(1)
	item = itemInterface.(ReaderNode)
	assert.Equal(t, 2, item.MaxHandlerPoolSize)
}

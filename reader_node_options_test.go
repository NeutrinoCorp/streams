package streamhub

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestWithHandler(t *testing.T) {
	opt := WithHandler(ReaderHandlerNoop{})
	assert.Implements(t, (*ReaderNodeOption)(nil), opt)

	hub := NewHub()
	hub.ReadByStreamKey("foo", opt)
	assert.NotNil(t,
		hub.readerSupervisor.readerRegistry[0].HandlerFunc)

	hub.ReadByStreamKey("bar")
	assert.Nil(t,
		hub.readerSupervisor.readerRegistry[1].HandlerFunc)
}

func TestWithHandlerFunc(t *testing.T) {
	opt := WithHandlerFunc(func(_ context.Context, _ Message) error {
		return nil
	})
	assert.Implements(t, (*ReaderNodeOption)(nil), opt)

	hub := NewHub()
	hub.ReadByStreamKey("foo", opt)
	assert.NotNil(t, hub.readerSupervisor.readerRegistry[0].HandlerFunc)

	hub.ReadByStreamKey("bar")
	assert.Nil(t,
		hub.readerSupervisor.readerRegistry[1].HandlerFunc)
}

func TestWithGroup(t *testing.T) {
	opt := WithGroup("bar-job")
	assert.Implements(t, (*ReaderNodeOption)(nil), opt)

	hub := NewHub()
	hub.ReadByStreamKey("foo", opt)
	assert.Equal(t, "bar-job", hub.readerSupervisor.readerRegistry[0].Group)
}

func TestWithConcurrencyLevel(t *testing.T) {
	opt := WithConcurrencyLevel(-1)
	assert.Implements(t, (*ReaderNodeOption)(nil), opt)

	hub := NewHub()
	hub.ReadByStreamKey("foo", opt)
	assert.Equal(t, 1, hub.readerSupervisor.readerRegistry[0].ConcurrencyLevel)

	opt = WithConcurrencyLevel(0)
	hub.ReadByStreamKey("bar", opt)
	assert.Equal(t, 1, hub.readerSupervisor.readerRegistry[1].ConcurrencyLevel)

	opt = WithConcurrencyLevel(2)
	hub.ReadByStreamKey("bar", opt)
	assert.Equal(t, 2, hub.readerSupervisor.readerRegistry[2].ConcurrencyLevel)
}

func TestWithRetryInitialInterval(t *testing.T) {
	opt := WithRetryInitialInterval(time.Second * 5)
	assert.Implements(t, (*ReaderNodeOption)(nil), opt)

	hub := NewHub()
	hub.ReadByStreamKey("foo", opt)
	assert.EqualValues(t, time.Second*5, hub.readerSupervisor.readerRegistry[0].RetryInitialInterval)
}

func TestWithRetryMaxInterval(t *testing.T) {
	opt := WithRetryMaxInterval(time.Second * 5)
	assert.Implements(t, (*ReaderNodeOption)(nil), opt)

	hub := NewHub()
	hub.ReadByStreamKey("foo", opt)
	assert.EqualValues(t, time.Second*5, hub.readerSupervisor.readerRegistry[0].RetryMaxInterval)
}

func TestWithRetryTimeout(t *testing.T) {
	opt := WithRetryTimeout(time.Second * 5)
	assert.Implements(t, (*ReaderNodeOption)(nil), opt)

	hub := NewHub()
	hub.ReadByStreamKey("foo", opt)
	assert.EqualValues(t, time.Second*5, hub.readerSupervisor.readerRegistry[0].RetryTimeout)
}

type fakeProviderCfg struct {
	Foo string
}

func TestWithProviderConfiguration(t *testing.T) {
	opt := WithProviderConfiguration(fakeProviderCfg{
		Foo: "foo",
	})
	assert.Implements(t, (*ReaderNodeOption)(nil), opt)

	hub := NewHub()
	hub.ReadByStreamKey("foo", opt)
	assert.EqualValues(t, fakeProviderCfg{
		Foo: "foo",
	}, hub.readerSupervisor.readerRegistry[0].ProviderConfiguration)
}

func TestWithDriver(t *testing.T) {
	opt := WithDriver(readerNoop{})
	assert.Implements(t, (*ReaderNodeOption)(nil), opt)

	hub := NewHub()
	hub.ReadByStreamKey("foo", opt)
	assert.EqualValues(t, readerNoop{},
		hub.readerSupervisor.readerRegistry[0].Reader)
}

func TestWithMaxHandlerPoolSize(t *testing.T) {
	opt := WithMaxHandlerPoolSize(-1)
	assert.Implements(t, (*ReaderNodeOption)(nil), opt)

	hub := NewHub()
	hub.ReadByStreamKey("foo", opt)
	assert.Equal(t, DefaultMaxHandlerPoolSize, hub.readerSupervisor.readerRegistry[0].MaxHandlerPoolSize)

	opt = WithMaxHandlerPoolSize(0)
	hub.ReadByStreamKey("bar", opt)
	assert.Equal(t, DefaultMaxHandlerPoolSize, hub.readerSupervisor.readerRegistry[1].MaxHandlerPoolSize)

	opt = WithMaxHandlerPoolSize(2)
	hub.ReadByStreamKey("bar", opt)
	assert.Equal(t, 2, hub.readerSupervisor.readerRegistry[2].MaxHandlerPoolSize)
}

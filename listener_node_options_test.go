package streamhub

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestWithListener(t *testing.T) {
	opt := WithListener(ListenerNoop{})
	assert.Implements(t, (*ListenerNodeOption)(nil), opt)

	hub := NewHub()
	hub.ListenByStreamKey("foo", opt)
	assert.NotNil(t,
		hub.listenerSupervisor.listenerRegistry[0].HandlerFunc)

	hub.ListenByStreamKey("bar")
	assert.Nil(t,
		hub.listenerSupervisor.listenerRegistry[1].HandlerFunc)
}

func TestWithListenerFunc(t *testing.T) {
	opt := WithListenerFunc(func(_ context.Context, _ Message) error {
		return nil
	})
	assert.Implements(t, (*ListenerNodeOption)(nil), opt)

	hub := NewHub()
	hub.ListenByStreamKey("foo", opt)
	assert.NotNil(t, hub.listenerSupervisor.listenerRegistry[0].HandlerFunc)

	hub.ListenByStreamKey("bar")
	assert.Nil(t,
		hub.listenerSupervisor.listenerRegistry[1].HandlerFunc)
}

func TestWithGroup(t *testing.T) {
	opt := WithGroup("bar-job")
	assert.Implements(t, (*ListenerNodeOption)(nil), opt)

	hub := NewHub()
	hub.ListenByStreamKey("foo", opt)
	assert.Equal(t, "bar-job", hub.listenerSupervisor.listenerRegistry[0].Group)
}

func TestWithConcurrencyLevel(t *testing.T) {
	opt := WithConcurrencyLevel(-1)
	assert.Implements(t, (*ListenerNodeOption)(nil), opt)

	hub := NewHub()
	hub.ListenByStreamKey("foo", opt)
	assert.Equal(t, 1, hub.listenerSupervisor.listenerRegistry[0].ConcurrencyLevel)

	opt = WithConcurrencyLevel(0)
	hub.ListenByStreamKey("bar", opt)
	assert.Equal(t, 1, hub.listenerSupervisor.listenerRegistry[1].ConcurrencyLevel)

	opt = WithConcurrencyLevel(2)
	hub.ListenByStreamKey("bar", opt)
	assert.Equal(t, 2, hub.listenerSupervisor.listenerRegistry[2].ConcurrencyLevel)
}

func TestWithRetryInitialInterval(t *testing.T) {
	opt := WithRetryInitialInterval(time.Second * 5)
	assert.Implements(t, (*ListenerNodeOption)(nil), opt)

	hub := NewHub()
	hub.ListenByStreamKey("foo", opt)
	assert.EqualValues(t, time.Second*5, hub.listenerSupervisor.listenerRegistry[0].RetryInitialInterval)
}

func TestWithRetryMaxInterval(t *testing.T) {
	opt := WithRetryMaxInterval(time.Second * 5)
	assert.Implements(t, (*ListenerNodeOption)(nil), opt)

	hub := NewHub()
	hub.ListenByStreamKey("foo", opt)
	assert.EqualValues(t, time.Second*5, hub.listenerSupervisor.listenerRegistry[0].RetryMaxInterval)
}

func TestWithRetryTimeout(t *testing.T) {
	opt := WithRetryTimeout(time.Second * 5)
	assert.Implements(t, (*ListenerNodeOption)(nil), opt)

	hub := NewHub()
	hub.ListenByStreamKey("foo", opt)
	assert.EqualValues(t, time.Second*5, hub.listenerSupervisor.listenerRegistry[0].RetryTimeout)
}

type fakeProviderCfg struct {
	Foo string
}

func TestWithProviderConfiguration(t *testing.T) {
	opt := WithProviderConfiguration(fakeProviderCfg{
		Foo: "foo",
	})
	assert.Implements(t, (*ListenerNodeOption)(nil), opt)

	hub := NewHub()
	hub.ListenByStreamKey("foo", opt)
	assert.EqualValues(t, fakeProviderCfg{
		Foo: "foo",
	}, hub.listenerSupervisor.listenerRegistry[0].ProviderConfiguration)
}

func TestWithDriver(t *testing.T) {
	opt := WithDriver(listenerDriverNoop{})
	assert.Implements(t, (*ListenerNodeOption)(nil), opt)

	hub := NewHub()
	hub.ListenByStreamKey("foo", opt)
	assert.EqualValues(t, listenerDriverNoop{},
		hub.listenerSupervisor.listenerRegistry[0].ListenerDriver)
}

func TestWithMaxHandlerPoolSize(t *testing.T) {
	opt := WithMaxHandlerPoolSize(-1)
	assert.Implements(t, (*ListenerNodeOption)(nil), opt)

	hub := NewHub()
	hub.ListenByStreamKey("foo", opt)
	assert.Equal(t, DefaultMaxHandlerPoolSize, hub.listenerSupervisor.listenerRegistry[0].MaxHandlerPoolSize)

	opt = WithMaxHandlerPoolSize(0)
	hub.ListenByStreamKey("bar", opt)
	assert.Equal(t, DefaultMaxHandlerPoolSize, hub.listenerSupervisor.listenerRegistry[1].MaxHandlerPoolSize)

	opt = WithMaxHandlerPoolSize(2)
	hub.ListenByStreamKey("bar", opt)
	assert.Equal(t, 2, hub.listenerSupervisor.listenerRegistry[2].MaxHandlerPoolSize)
}

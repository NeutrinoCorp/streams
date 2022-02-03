package streamhub

import "time"

type listenerNodeOptions struct {
	listener              Listener
	listenerFunc          ListenerFunc
	group                 string
	concurrencyLevel      int
	maxRetries            uint32
	retryBackoff          time.Duration
	retryTimeout          time.Duration
	providerConfiguration interface{}
	driver                ListenerDriver
}

// ListenerNodeOption enables configuration of a ListenerNode.
type ListenerNodeOption interface {
	apply(*listenerNodeOptions)
}

type listenerOption struct {
	Listener Listener
}

func (o listenerOption) apply(opts *listenerNodeOptions) {
	opts.listener = o.Listener
}

// WithListener sets the Listener of a ListenerNode.
func WithListener(l Listener) ListenerNodeOption {
	return listenerOption{Listener: l}
}

type listenerFuncOption struct {
	ListenerFunc ListenerFunc
}

func (o listenerFuncOption) apply(opts *listenerNodeOptions) {
	opts.listenerFunc = o.ListenerFunc
}

// WithListenerFunc sets the ListenerFunc of a ListenerNode.
func WithListenerFunc(l ListenerFunc) ListenerNodeOption {
	return listenerFuncOption{ListenerFunc: l}
}

type groupOption struct {
	ConsumerGroup string
}

func (o groupOption) apply(opts *listenerNodeOptions) {
	opts.group = o.ConsumerGroup
}

// WithGroup sets the consumer group or queue name of a ListenerNode.
//
// Note: It may not be available for some providers.
func WithGroup(g string) ListenerNodeOption {
	return groupOption{ConsumerGroup: g}
}

type concurrencyLevelOption struct {
	ConcurrencyLevel int
}

func (o concurrencyLevelOption) apply(opts *listenerNodeOptions) {
	opts.concurrencyLevel = o.ConcurrencyLevel
}

// WithConcurrencyLevel sets the concurrency level of a ListenerNode. In other words, jobs to be scheduled by
// the ListenerNode.
//
// Note: If level was defined less or equal than 0, the ListenerNode will schedule 1 job
func WithConcurrencyLevel(n int) ListenerNodeOption {
	if n <= 0 {
		n = 1
	}
	return concurrencyLevelOption{ConcurrencyLevel: n}
}

type maxRetriesOption struct {
	MaxRetries uint32
}

func (o maxRetriesOption) apply(opts *listenerNodeOptions) {
	opts.maxRetries = o.MaxRetries
}

// WithMaxRetries sets the maximum amount of retying tasks of a ListenerNode.
// If overpassed, the ListenerNode will stop scheduling stream-listening tasks.
func WithMaxRetries(n uint32) ListenerNodeOption {
	return maxRetriesOption{MaxRetries: n}
}

type retryBackoffOption struct {
	RetryBackoff time.Duration
}

func (o retryBackoffOption) apply(opts *listenerNodeOptions) {
	opts.retryBackoff = o.RetryBackoff
}

// WithRetryBackoff sets the duration interval for each retying tasks of a ListenerNode.
func WithRetryBackoff(d time.Duration) ListenerNodeOption {
	return retryBackoffOption{RetryBackoff: d}
}

type retryTimeoutOption struct {
	RetryTimeout time.Duration
}

func (o retryTimeoutOption) apply(opts *listenerNodeOptions) {
	opts.retryTimeout = o.RetryTimeout
}

// WithRetryTimeout sets the maximum duration for retying tasks of a ListenerNode.
func WithRetryTimeout(d time.Duration) ListenerNodeOption {
	return retryTimeoutOption{RetryTimeout: d}
}

type providerConfigurationOption struct {
	ProviderConfiguration interface{}
}

func (o providerConfigurationOption) apply(opts *listenerNodeOptions) {
	opts.providerConfiguration = o.ProviderConfiguration
}

// WithProviderConfiguration sets the custom provider configuration of a ListenerNode (e.g. aws.Config, sarama.Config).
func WithProviderConfiguration(cfg interface{}) ListenerNodeOption {
	return providerConfigurationOption{ProviderConfiguration: cfg}
}

type driverOption struct {
	Driver ListenerDriver
}

func (o driverOption) apply(opts *listenerNodeOptions) {
	opts.driver = o.Driver
}

// WithDriver sets the driver of a ListenerNode (e.g. Apache Kafka, Apache Pulsar, Amazon SQS).
func WithDriver(d ListenerDriver) ListenerNodeOption {
	return driverOption{Driver: d}
}

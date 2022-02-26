package streamhub

import "time"

type listenerNodeOptions struct {
	hosts                 []string
	listener              Listener
	listenerFunc          ListenerFunc
	group                 string
	retryStream           string
	deadLetterStream      string
	concurrencyLevel      int
	retryInitialInterval  time.Duration
	retryMaxInterval      time.Duration
	retryTimeout          time.Duration
	providerConfiguration interface{}
	driver                ListenerDriver
	maxHandlerPoolSize    int
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

type retryInitialIntervalOption struct {
	RetryInitialInterval time.Duration
}

func (o retryInitialIntervalOption) apply(opts *listenerNodeOptions) {
	opts.retryInitialInterval = o.RetryInitialInterval
}

// WithRetryInitialInterval sets the initial duration interval for each retying tasks of a ListenerNode.
func WithRetryInitialInterval(d time.Duration) ListenerNodeOption {
	return retryInitialIntervalOption{RetryInitialInterval: d}
}

type retryMaxIntervalOption struct {
	RetryMaxInterval time.Duration
}

func (o retryMaxIntervalOption) apply(opts *listenerNodeOptions) {
	opts.retryMaxInterval = o.RetryMaxInterval
}

// WithRetryMaxInterval sets the maximum duration interval for each retying tasks of a ListenerNode.
func WithRetryMaxInterval(d time.Duration) ListenerNodeOption {
	return retryMaxIntervalOption{RetryMaxInterval: d}
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

type maxHandlerPoolSizeOption struct {
	PoolSize int
}

func (o maxHandlerPoolSizeOption) apply(opts *listenerNodeOptions) {
	opts.maxHandlerPoolSize = o.PoolSize
}

// WithMaxHandlerPoolSize sets the maximum number of goroutines executed by a ListenerNode's Listener or ListenerFunc.
//
// Note: If size was defined less or equal than 0, the ListenerNode internal implementations will allocate a semaphore
// of 10 goroutines per handler.
func WithMaxHandlerPoolSize(n int) ListenerNodeOption {
	if n <= 0 {
		n = DefaultMaxHandlerPoolSize
	}
	return maxHandlerPoolSizeOption{PoolSize: n}
}

type hostsOption struct {
	Hosts []string
}

func (o hostsOption) apply(opts *listenerNodeOptions) {
	opts.hosts = o.Hosts
}

// WithHosts sets the infrastructure host addresses used by a ListenerNode
func WithHosts(addr ...string) ListenerNodeOption {
	return hostsOption{Hosts: addr}
}

type retryStreamOption struct {
	Stream string
}

func (o retryStreamOption) apply(opts *listenerNodeOptions) {
	opts.retryStream = o.Stream
}

// WithRetryStream sets the retry stream for a ListenerNode
func WithRetryStream(s string) ListenerNodeOption {
	return retryStreamOption{Stream: s}
}

type deadLetterStreamOption struct {
	Stream string
}

func (o deadLetterStreamOption) apply(opts *listenerNodeOptions) {
	opts.deadLetterStream = o.Stream
}

// WithDeadLetterStream sets the dead-letter stream for a ListenerNode
func WithDeadLetterStream(s string) ListenerNodeOption {
	return deadLetterStreamOption{Stream: s}
}

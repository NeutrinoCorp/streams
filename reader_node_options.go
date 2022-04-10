package streamhub

import "time"

type readerNodeOptions struct {
	readerHandler         ReaderHandler
	readerFunc            ReaderHandleFunc
	group                 string
	concurrencyLevel      int
	retryInitialInterval  time.Duration
	retryMaxInterval      time.Duration
	retryTimeout          time.Duration
	providerConfiguration interface{}
	driver                Reader
	maxHandlerPoolSize    int
}

// ReaderNodeOption enables configuration of a ReaderNode.
type ReaderNodeOption interface {
	apply(*readerNodeOptions)
}

type readerHandlerOption struct {
	ReaderHandler ReaderHandler
}

func (o readerHandlerOption) apply(opts *readerNodeOptions) {
	opts.readerHandler = o.ReaderHandler
}

// WithHandler sets the ReaderHandler of a ReaderNode.
func WithHandler(l ReaderHandler) ReaderNodeOption {
	return readerHandlerOption{ReaderHandler: l}
}

type readerFuncOption struct {
	ReaderFunc ReaderHandleFunc
}

func (o readerFuncOption) apply(opts *readerNodeOptions) {
	opts.readerFunc = o.ReaderFunc
}

// WithHandlerFunc sets the ReaderHandleFunc of a ReaderNode.
func WithHandlerFunc(l ReaderHandleFunc) ReaderNodeOption {
	return readerFuncOption{ReaderFunc: l}
}

type groupOption struct {
	ConsumerGroup string
}

func (o groupOption) apply(opts *readerNodeOptions) {
	opts.group = o.ConsumerGroup
}

// WithGroup sets the consumer group or queue name of a ReaderNode.
//
// Note: It may not be available for some providers.
func WithGroup(g string) ReaderNodeOption {
	return groupOption{ConsumerGroup: g}
}

type concurrencyLevelOption struct {
	ConcurrencyLevel int
}

func (o concurrencyLevelOption) apply(opts *readerNodeOptions) {
	opts.concurrencyLevel = o.ConcurrencyLevel
}

// WithConcurrencyLevel sets the concurrency level of a ReaderNode. In other words, jobs to be scheduled by
// the ReaderNode.
//
// Note: If level was defined less or equal than 0, the ReaderNode will schedule 1 job
func WithConcurrencyLevel(n int) ReaderNodeOption {
	if n <= 0 {
		n = 1
	}
	return concurrencyLevelOption{ConcurrencyLevel: n}
}

type retryInitialIntervalOption struct {
	RetryInitialInterval time.Duration
}

func (o retryInitialIntervalOption) apply(opts *readerNodeOptions) {
	opts.retryInitialInterval = o.RetryInitialInterval
}

// WithRetryInitialInterval sets the initial duration interval for each retying tasks of a ReaderNode.
func WithRetryInitialInterval(d time.Duration) ReaderNodeOption {
	return retryInitialIntervalOption{RetryInitialInterval: d}
}

type retryMaxIntervalOption struct {
	RetryMaxInterval time.Duration
}

func (o retryMaxIntervalOption) apply(opts *readerNodeOptions) {
	opts.retryMaxInterval = o.RetryMaxInterval
}

// WithRetryMaxInterval sets the maximum duration interval for each retying tasks of a ReaderNode.
func WithRetryMaxInterval(d time.Duration) ReaderNodeOption {
	return retryMaxIntervalOption{RetryMaxInterval: d}
}

type retryTimeoutOption struct {
	RetryTimeout time.Duration
}

func (o retryTimeoutOption) apply(opts *readerNodeOptions) {
	opts.retryTimeout = o.RetryTimeout
}

// WithRetryTimeout sets the maximum duration for retying tasks of a ReaderNode.
func WithRetryTimeout(d time.Duration) ReaderNodeOption {
	return retryTimeoutOption{RetryTimeout: d}
}

type providerConfigurationOption struct {
	ProviderConfiguration interface{}
}

func (o providerConfigurationOption) apply(opts *readerNodeOptions) {
	opts.providerConfiguration = o.ProviderConfiguration
}

// WithProviderConfiguration sets the custom provider configuration of a ReaderNode (e.g. aws.Config, sarama.Config).
func WithProviderConfiguration(cfg interface{}) ReaderNodeOption {
	return providerConfigurationOption{ProviderConfiguration: cfg}
}

type driverOption struct {
	Driver Reader
}

func (o driverOption) apply(opts *readerNodeOptions) {
	opts.driver = o.Driver
}

// WithDriver sets the driver of a ReaderNode (e.g. Apache Kafka, Apache Pulsar, Amazon SQS).
func WithDriver(d Reader) ReaderNodeOption {
	return driverOption{Driver: d}
}

type maxHandlerPoolSizeOption struct {
	PoolSize int
}

func (o maxHandlerPoolSizeOption) apply(opts *readerNodeOptions) {
	opts.maxHandlerPoolSize = o.PoolSize
}

// WithMaxHandlerPoolSize sets the maximum number of goroutines executed by a ReaderNode's Reader or ReaderFunc.
//
// Note: If size was defined less or equal than 0, the ReaderNode internal implementations will allocate a semaphore
// of 10 goroutines per handler.
func WithMaxHandlerPoolSize(n int) ReaderNodeOption {
	if n <= 0 {
		n = DefaultMaxHandlerPoolSize
	}
	return maxHandlerPoolSizeOption{PoolSize: n}
}

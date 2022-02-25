package streamhub

import "errors"

var (
	// ErrInvalidProviderConfiguration the configuration provided for a Third-party Driver was not valid
	ErrInvalidProviderConfiguration = errors.New("streamhub: Invalid provider configuration")
	// ErrMissingPublisherDriver no publisher driver was found
	ErrMissingPublisherDriver = errors.New("streamhub: Missing publisher driver")
)

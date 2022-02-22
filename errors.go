package streamhub

import "errors"

// ErrMissingPublisherDriver no publisher driver was found
var ErrMissingPublisherDriver = errors.New("streamhub: Missing publisher driver")

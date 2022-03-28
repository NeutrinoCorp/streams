package streamhub

import "errors"

// ErrMissingWriterDriver no publisher driver was found.
var ErrMissingWriterDriver = errors.New("streamhub: Missing writer driver")

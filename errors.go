package streams

import "errors"

// ErrMissingWriterDriver no publisher driver was found.
var ErrMissingWriterDriver = errors.New("streams: Missing writer driver")

package streams

// readerRegistry is an in-memory database (queue) of stream-reading background jobs requested by the given system.
// key: stream
type readerRegistry map[string][]ReaderNode

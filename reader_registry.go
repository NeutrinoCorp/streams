package streamhub

// readerRegistry is an in-memory database (queue) of stream-reading background jobs requested by the given system.
type readerRegistry []ReaderNode

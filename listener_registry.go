package streamhub

// listenerRegistry is an in-memory database (queue) of stream-listening background jobs requested by the given system.
type listenerRegistry []ListenerNode

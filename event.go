package streamhub

// Event is an abstract message unit used by streamhub-based systems to publish messages with a `subject` populated field
// of a Message.
type Event interface {
	// Subject This describes the subject of the event in the context of the event producer (identified by source).
	// In publish-subscribe scenarios, a subscriber will typically subscribe to events emitted by a source, but the
	// source identifier alone might not be sufficient as a qualifier for any specific event if the source
	// context has internal sub-structure.
	//
	// Identifying the subject of the event in context metadata (opposed to only in the data payload) is particularly
	// helpful in generic subscription filtering scenarios where middleware is unable to interpret the data content.
	// In the above example, the subscriber might only be interested in blobs with names ending with '.jpg' or '.jpeg'
	// and the subject attribute allows for constructing a simple and efficient string-suffix filter for that
	// subset of events.
	Subject() string
}

package streams

import (
	"errors"

	"github.com/emirpasic/gods/maps"
	"github.com/emirpasic/gods/maps/treebidimap"
	"github.com/emirpasic/gods/utils"
	"github.com/modern-go/reflect2"
)

var (
	ErrStreamNotFound = errors.New("streams: Stream not found")
)

type StreamMetadata struct {
	Name      string
	TypeOf    string
	SchemaURL string
	GoType    reflect2.Type
}

type StreamRegistry struct {
	// key -> StreamMetadata Name, TypeOf message
	// val -> StreamMetadata metadata
	buf maps.BidiMap
}

var streamComparator utils.Comparator = func(a, b interface{}) int {
	s1, s2 := a.(StreamMetadata), b.(StreamMetadata)
	return utils.StringComparator(s1.TypeOf, s2.TypeOf)
}

func NewStreamRegistry() *StreamRegistry {
	return &StreamRegistry{
		buf: treebidimap.NewWith(utils.StringComparator, streamComparator),
	}
}

func (s StreamRegistry) Set(stream string, message interface{}, opts ...StreamRegistryOption) {
	typeof := reflect2.TypeOf(message)
	metadata := StreamMetadata{
		Name:   stream,
		GoType: typeof,
		TypeOf: typeof.String(),
	}
	for _, opt := range opts {
		opt.apply(&metadata)
	}
	s.buf.Put(stream, metadata)
}

func (s StreamRegistry) Get(key string) (StreamMetadata, error) {
	streamMetadataInter, ok := s.buf.Get(key)
	if !ok {
		return StreamMetadata{}, ErrStreamNotFound
	}
	return streamMetadataInter.(StreamMetadata), nil
}

func (s StreamRegistry) GetByType(message interface{}) (StreamMetadata, error) {
	key, ok := s.buf.GetKey(StreamMetadata{TypeOf: reflect2.TypeOf(message).String()})
	if !ok {
		return StreamMetadata{}, ErrStreamNotFound
	}
	streamMetadataInter, ok := s.buf.Get(key)
	if !ok {
		return StreamMetadata{}, ErrStreamNotFound
	}
	return streamMetadataInter.(StreamMetadata), nil
}

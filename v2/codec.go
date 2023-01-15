package streams

import (
	"bytes"

	jsoniter "github.com/json-iterator/go"
)

const (
	codecAppTypeJSON     = "application/json"
	codecAppTypeProtobuf = "application/octet-stream"
	codecAppTypeXML      = "application/xml"
	codecAppTypeAvro     = "application/avro"
)

// Encoder converts given data types to a codec-specific data type.
type Encoder interface {
	Marshal(interface{}) ([]byte, error)
}

// MultiEncoder converts a set of given data types to a set of codec-specific data type using '\n' as escape sequence.
//
// Use bufio.Scanner (or similar) to read lines written by this Encoder.
type MultiEncoder interface {
	MarshalMany(values []interface{}) ([]byte, error)
}

// Decoder converts codec-specific data types to the given data type.
type Decoder interface {
	// Unmarshal decodes p into v value. If v is not a pointer reference, then no changes will get back to
	// the caller as decoding will be modifying a copy of v instead the reference.
	Unmarshal(p []byte, v interface{}) error
}

// Codec converts given data types to a codec-specific data type and vice-versa.
type Codec interface {
	Encoder
	Decoder
	GetContentType() string
}

type JSONCodec struct{}

var _ Codec = JSONCodec{}

func (j JSONCodec) GetContentType() string {
	return codecAppTypeJSON
}

func (j JSONCodec) Marshal(v interface{}) ([]byte, error) {
	return jsoniter.Marshal(v)
}

func (j JSONCodec) Unmarshal(p []byte, v interface{}) error {
	return jsoniter.Unmarshal(p, v)
}

// JSONCodecBuffered codec implementing JSON encoding and decoding. This differs from JSONCodec as it uses
// two internal dynamic bytes.Buffer and json.Encoder and json.Decoder to reuse allocated space from multiple calls,
// optimizing space and time complexity. Moreover, an implementation of MultiEncoder is contained for more
// complex scenarios where multiple JSON objects are required to be writen and separated each by new lines using
// '\n' as escaping character.
//
// Every call to Unmarshal, Marshal and MarshalMany will flush the corresponding buffer (read/write) after
// the decoding/encoding process was properly executed.
//
// NOTE: This struct is not thread-safe and shall be used along a locking mechanism on top in order to avoid
// race condition scenarios.
//
// NOTE: Underlying implementations replicate json.Encoder and json.Decoder behaviors.
type JSONCodecBuffered struct {
	writeBuf, readBuf *bytes.Buffer
	encoder           *jsoniter.Encoder
	decoder           *jsoniter.Decoder
}

var _ Codec = JSONCodecBuffered{}

var _ MultiEncoder = JSONCodecBuffered{}

func NewJSONCodecBuffered(writeBufCap, readBufCap int) *JSONCodecBuffered {
	writeBuf, readBuf := &bytes.Buffer{}, &bytes.Buffer{}
	if writeBufCap > 0 {
		writeBuf.Grow(writeBufCap)
	}
	if readBufCap > 0 {
		readBuf.Grow(readBufCap)
	}
	return &JSONCodecBuffered{
		writeBuf: writeBuf,
		readBuf:  readBuf,
		encoder:  jsoniter.NewEncoder(writeBuf),
		decoder:  jsoniter.NewDecoder(readBuf),
	}
}

func (j JSONCodecBuffered) GetContentType() string {
	return codecAppTypeJSON
}

func (j JSONCodecBuffered) Marshal(v interface{}) (out []byte, err error) {
	defer j.writeBuf.Reset()
	err = j.encoder.Encode(v)
	out = make([]byte, j.writeBuf.Len()-1, j.writeBuf.Cap())
	_, _ = j.writeBuf.Read(out)
	//out = out[:len(out)-1] // avoids extra malloc, removes \n escaping char
	return
}

func (j JSONCodecBuffered) MarshalMany(values []interface{}) ([]byte, error) {
	if len(values) == 0 {
		return nil, nil
	}

	defer j.writeBuf.Reset()
	for _, v := range values {
		if err := j.encoder.Encode(v); err != nil {
			return nil, err
		}
	}
	return j.writeBuf.Bytes(), nil
}

func (j JSONCodecBuffered) Unmarshal(p []byte, v interface{}) error {
	if len(p) == 0 {
		return nil
	}

	defer j.readBuf.Reset()
	j.readBuf.Write(p)
	return j.decoder.Decode(v)
}

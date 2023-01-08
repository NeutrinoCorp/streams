package streams_test

import (
	"errors"
	"testing"

	"github.com/modern-go/reflect2"
	"github.com/neutrinocorp/streams/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestJSONCodec_Marshal(t *testing.T) {
	// NOTE: Using global encoders could cause race conditions as JSON codec implementations are not thread-safe.
	var globalEncoder, globalBufferedEncoder streams.Encoder = streams.JSONCodec{}, streams.NewJSONCodecBuffered(0, 0)
	tests := []struct {
		name    string
		in      []interface{}
		encoder streams.Encoder
		exp     []string
	}{
		{
			name:    "nil",
			in:      nil,
			encoder: globalEncoder,
			exp:     nil,
		},
		{
			name:    "empty",
			in:      []interface{}{},
			encoder: globalEncoder,
			exp:     nil,
		},
		{
			name: "single empty",
			in: []interface{}{
				fakeMessage{
					ID:             "",
					CorrelationID:  "",
					CausationID:    "",
					StreamName:     "",
					StreamVersion:  0,
					StreamFullName: "",
					SchemaURL:      "",
					Data:           nil,
					Metadata:       nil,
				},
			},
			encoder: globalEncoder,
			exp: []string{
				"{\"message_id\":\"\",\"correlation_id\":\"\",\"causation_id\":\"\",\"stream_name\":\"\",\"stream_version\":0,\"stream_full_name\":\"\",\"schema_url\":\"\",\"data\":null,\"metadata\":null}",
			},
		},
		{
			name: "single",
			in: []interface{}{
				fakeMessage{
					ID:             "123",
					CorrelationID:  "123",
					CausationID:    "123",
					StreamName:     "neutrino.places.foo.raw",
					StreamVersion:  2,
					StreamFullName: "neutrino.places.foo.raw.v2",
					SchemaURL:      "https://events.docs.neutrinocorp.org/places#foo_raw",
					Data:           []byte("this is a test 0"),
					Metadata: map[string]string{
						// DO NOT add more fields as maps have an unpredictable item ordering by nature;
						// multiple items will give test cases false positives results and vice-versa.
						//
						// Thus, we use only a single item to test marshaling capacities for map types.
						"example": "lorem ipsum",
					},
				},
			},
			encoder: globalEncoder,
			exp: []string{
				"{\"message_id\":\"123\",\"correlation_id\":\"123\",\"causation_id\":\"123\",\"stream_name\":\"neutrino.places.foo.raw\",\"stream_version\":2,\"stream_full_name\":\"neutrino.places.foo.raw.v2\",\"schema_url\":\"https://events.docs.neutrinocorp.org/places#foo_raw\",\"data\":\"dGhpcyBpcyBhIHRlc3QgMA==\",\"metadata\":{\"example\":\"lorem ipsum\"}}",
			},
		},
		{
			name:    "buffered nil",
			in:      nil,
			encoder: globalBufferedEncoder,
			exp:     nil,
		},
		{
			name: "buffered empty",
			in: []interface{}{
				fakeMessage{
					ID:             "",
					CorrelationID:  "",
					CausationID:    "",
					StreamName:     "",
					StreamVersion:  0,
					StreamFullName: "",
					SchemaURL:      "",
					Data:           nil,
					Metadata:       nil,
				},
			},
			encoder: globalBufferedEncoder,
			exp: []string{
				"{\"message_id\":\"\",\"correlation_id\":\"\",\"causation_id\":\"\",\"stream_name\":\"\",\"stream_version\":0,\"stream_full_name\":\"\",\"schema_url\":\"\",\"data\":null,\"metadata\":null}",
			},
		},
		{
			name: "buffered multiple",
			in: []interface{}{
				fakeMessage{
					ID:             "123",
					CorrelationID:  "123",
					CausationID:    "123",
					StreamName:     "neutrino.places.foo.raw",
					StreamVersion:  2,
					StreamFullName: "neutrino.places.foo.raw.v2",
					SchemaURL:      "https://events.docs.neutrinocorp.org/places#foo_raw",
					Data:           []byte("this is a test 0"),
					Metadata: map[string]string{
						"example": "lorem ipsum",
					},
				},
				fakeMessage{
					ID:             "456",
					CorrelationID:  "123",
					CausationID:    "123",
					StreamName:     "neutrino.places.foo.raw",
					StreamVersion:  2,
					StreamFullName: "neutrino.places.foo.raw.v2",
					SchemaURL:      "https://events.docs.neutrinocorp.org/places#foo_raw",
					Data:           []byte("this is a test 1"),
					Metadata:       nil,
				},
				fakeMessage{
					ID:             "789",
					CorrelationID:  "123",
					CausationID:    "456",
					StreamName:     "neutrino.places.foo.raw",
					StreamVersion:  2,
					StreamFullName: "neutrino.places.foo.raw.v2",
					SchemaURL:      "https://events.docs.neutrinocorp.org/places#foo_raw",
					Data:           []byte("this is a test 2"),
					Metadata:       nil,
				},
			},
			encoder: globalBufferedEncoder,
			exp: []string{
				"{\"message_id\":\"123\",\"correlation_id\":\"123\",\"causation_id\":\"123\",\"stream_name\":\"neutrino.places.foo.raw\",\"stream_version\":2,\"stream_full_name\":\"neutrino.places.foo.raw.v2\",\"schema_url\":\"https://events.docs.neutrinocorp.org/places#foo_raw\",\"data\":\"dGhpcyBpcyBhIHRlc3QgMA==\",\"metadata\":{\"example\":\"lorem ipsum\"}}",
				"{\"message_id\":\"456\",\"correlation_id\":\"123\",\"causation_id\":\"123\",\"stream_name\":\"neutrino.places.foo.raw\",\"stream_version\":2,\"stream_full_name\":\"neutrino.places.foo.raw.v2\",\"schema_url\":\"https://events.docs.neutrinocorp.org/places#foo_raw\",\"data\":\"dGhpcyBpcyBhIHRlc3QgMQ==\",\"metadata\":null}",
				"{\"message_id\":\"789\",\"correlation_id\":\"123\",\"causation_id\":\"456\",\"stream_name\":\"neutrino.places.foo.raw\",\"stream_version\":2,\"stream_full_name\":\"neutrino.places.foo.raw.v2\",\"schema_url\":\"https://events.docs.neutrinocorp.org/places#foo_raw\",\"data\":\"dGhpcyBpcyBhIHRlc3QgMg==\",\"metadata\":null}",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			for i, msg := range tt.in {
				out, _ := tt.encoder.Marshal(msg)
				assert.Equal(t, tt.exp[i], string(out))
			}
		})
	}
}

func TestJSONCodecBuffered_MarshalMany(t *testing.T) {
	globalEncoder := streams.NewJSONCodecBuffered(0, 0)
	tests := []struct {
		name    string
		encoder streams.MultiEncoder
		in      []interface{}
		exp     string
	}{
		{
			name:    "nil",
			encoder: globalEncoder,
			in:      nil,
			exp:     "",
		},
		{
			name:    "empty",
			encoder: globalEncoder,
			in:      []interface{}{},
			exp:     "",
		},
		{
			name:    "single",
			encoder: globalEncoder,
			in: []interface{}{
				fakeMessage{
					ID:             "123",
					CorrelationID:  "123",
					CausationID:    "123",
					StreamName:     "neutrino.places.foo.raw",
					StreamVersion:  2,
					StreamFullName: "neutrino.places.foo.raw.v2",
					SchemaURL:      "https://events.docs.neutrinocorp.org/places#foo_raw",
					Data:           []byte("this is a test 0"),
					Metadata: map[string]string{
						// DO NOT add more fields as maps have an unpredictable item ordering by nature;
						// multiple items will give test cases false positives results and vice-versa.
						//
						// Thus, we use only a single item to test marshaling capacities for map types.
						"example": "lorem ipsum",
					},
				},
			},
			exp: "{\"message_id\":\"123\",\"correlation_id\":\"123\",\"causation_id\":\"123\",\"stream_name\":\"neutrino.places.foo.raw\",\"stream_version\":2,\"stream_full_name\":\"neutrino.places.foo.raw.v2\",\"schema_url\":\"https://events.docs.neutrinocorp.org/places#foo_raw\",\"data\":\"dGhpcyBpcyBhIHRlc3QgMA==\",\"metadata\":{\"example\":\"lorem ipsum\"}}\n",
		},
		{
			name:    "multiple",
			encoder: globalEncoder,
			in: []interface{}{
				fakeMessage{
					ID:             "123",
					CorrelationID:  "123",
					CausationID:    "123",
					StreamName:     "neutrino.places.foo.raw",
					StreamVersion:  2,
					StreamFullName: "neutrino.places.foo.raw.v2",
					SchemaURL:      "https://events.docs.neutrinocorp.org/places#foo_raw",
					Data:           []byte("this is a test 0"),
					Metadata: map[string]string{
						// DO NOT add more fields as maps have an unpredictable item ordering by nature;
						// multiple items will give test cases false positives results and vice-versa.
						//
						// Thus, we use only a single item to test marshaling capacities for map types.
						"example": "lorem ipsum",
					},
				},
				fakeMessage{
					ID:             "456",
					CorrelationID:  "123",
					CausationID:    "123",
					StreamName:     "neutrino.places.foo.raw",
					StreamVersion:  2,
					StreamFullName: "neutrino.places.foo.raw.v2",
					SchemaURL:      "https://events.docs.neutrinocorp.org/places#foo_raw",
					Data:           []byte("this is a test 1"),
					Metadata:       nil,
				},
				fakeMessage{
					ID:             "789",
					CorrelationID:  "123",
					CausationID:    "456",
					StreamName:     "neutrino.places.foo.raw",
					StreamVersion:  2,
					StreamFullName: "neutrino.places.foo.raw.v2",
					SchemaURL:      "https://events.docs.neutrinocorp.org/places#foo_raw",
					Data:           []byte("this is a test 2"),
					Metadata:       nil,
				},
			},
			exp: "{\"message_id\":\"123\",\"correlation_id\":\"123\",\"causation_id\":\"123\",\"stream_name\":\"neutrino.places.foo.raw\",\"stream_version\":2,\"stream_full_name\":\"neutrino.places.foo.raw.v2\",\"schema_url\":\"https://events.docs.neutrinocorp.org/places#foo_raw\",\"data\":\"dGhpcyBpcyBhIHRlc3QgMA==\",\"metadata\":{\"example\":\"lorem ipsum\"}}\n{\"message_id\":\"456\",\"correlation_id\":\"123\",\"causation_id\":\"123\",\"stream_name\":\"neutrino.places.foo.raw\",\"stream_version\":2,\"stream_full_name\":\"neutrino.places.foo.raw.v2\",\"schema_url\":\"https://events.docs.neutrinocorp.org/places#foo_raw\",\"data\":\"dGhpcyBpcyBhIHRlc3QgMQ==\",\"metadata\":null}\n{\"message_id\":\"789\",\"correlation_id\":\"123\",\"causation_id\":\"456\",\"stream_name\":\"neutrino.places.foo.raw\",\"stream_version\":2,\"stream_full_name\":\"neutrino.places.foo.raw.v2\",\"schema_url\":\"https://events.docs.neutrinocorp.org/places#foo_raw\",\"data\":\"dGhpcyBpcyBhIHRlc3QgMg==\",\"metadata\":null}\n",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			out, _ := tt.encoder.MarshalMany(tt.in)
			assert.Equal(t, tt.exp, string(out))
		})
	}
}

func TestJSONCodec_Unmarshal(t *testing.T) {
	// NOTE: Using global encoders could cause race conditions as JSON codec implementations are not thread-safe.
	var globalDecoder, globalBufferedDecoder streams.Decoder = streams.JSONCodec{}, streams.NewJSONCodecBuffered(0, 0)

	tests := []struct {
		name    string
		decoder streams.Decoder
		in      []byte
		err     error
		exp     interface{}
	}{
		{
			name:    "empty",
			decoder: globalDecoder,
			in:      nil,
			err:     errors.New("Read: unexpected value type: 0, error found in #0 byte of ...||..., bigger context ...||..."),
			exp:     nil,
		},
		{
			name:    "single",
			decoder: globalDecoder,
			in:      []byte("{\"message_id\":\"123\",\"correlation_id\":\"123\",\"causation_id\":\"123\",\"stream_name\":\"neutrino.places.foo.raw\",\"stream_version\":2,\"stream_full_name\":\"neutrino.places.foo.raw.v2\",\"schema_url\":\"https://events.docs.neutrinocorp.org/places#foo_raw\",\"data\":\"dGhpcyBpcyBhIHRlc3QgMA==\",\"metadata\":{\"example\":\"lorem ipsum\"}}"),
			err:     nil,
			exp: fakeMessage{
				ID:             "123",
				CorrelationID:  "123",
				CausationID:    "123",
				StreamName:     "neutrino.places.foo.raw",
				StreamVersion:  2,
				StreamFullName: "neutrino.places.foo.raw.v2",
				SchemaURL:      "https://events.docs.neutrinocorp.org/places#foo_raw",
				Data:           []byte("this is a test 0"),
				Metadata: map[string]string{
					"example": "lorem ipsum",
				},
			},
		},
		{
			name:    "buffered empty",
			decoder: globalBufferedDecoder,
			in:      nil,
			err:     nil,
			exp:     nil,
		},
		{
			name:    "buffered single",
			decoder: globalBufferedDecoder,
			in:      []byte("{\"message_id\":\"123\",\"correlation_id\":\"123\",\"causation_id\":\"123\",\"stream_name\":\"neutrino.places.foo.raw\",\"stream_version\":2,\"stream_full_name\":\"neutrino.places.foo.raw.v2\",\"schema_url\":\"https://events.docs.neutrinocorp.org/places#foo_raw\",\"data\":\"dGhpcyBpcyBhIHRlc3QgMA==\",\"metadata\":{\"example\":\"lorem ipsum\"}}"),
			err:     nil,
			exp: fakeMessage{
				ID:             "123",
				CorrelationID:  "123",
				CausationID:    "123",
				StreamName:     "neutrino.places.foo.raw",
				StreamVersion:  2,
				StreamFullName: "neutrino.places.foo.raw.v2",
				SchemaURL:      "https://events.docs.neutrinocorp.org/places#foo_raw",
				Data:           []byte("this is a test 0"),
				Metadata: map[string]string{
					"example": "lorem ipsum",
				},
			},
		},
		{
			name:    "buffered check not EOF",
			decoder: globalBufferedDecoder,
			in:      []byte("{\"message_id\":\"123\",\"correlation_id\":\"123\",\"causation_id\":\"123\",\"stream_name\":\"neutrino.places.foo.raw\",\"stream_version\":2,\"stream_full_name\":\"neutrino.places.foo.raw.v2\",\"schema_url\":\"https://events.docs.neutrinocorp.org/places#foo_raw\",\"data\":\"dGhpcyBpcyBhIHRlc3QgMA==\",\"metadata\":{\"example\":\"lorem ipsum\"}}"),
			err:     nil,
			exp: fakeMessage{
				ID:             "123",
				CorrelationID:  "123",
				CausationID:    "123",
				StreamName:     "neutrino.places.foo.raw",
				StreamVersion:  2,
				StreamFullName: "neutrino.places.foo.raw.v2",
				SchemaURL:      "https://events.docs.neutrinocorp.org/places#foo_raw",
				Data:           []byte("this is a test 0"),
				Metadata: map[string]string{
					"example": "lorem ipsum",
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.exp == nil {
				assert.Equal(t, tt.err, tt.decoder.Unmarshal(tt.in, &tt.exp))
				return
			}

			expType := reflect2.TypeOf(tt.exp)
			expZeroVal := expType.New()
			require.Equal(t, tt.err, tt.decoder.Unmarshal(tt.in, expZeroVal))
			// TypeOf.Indirect() removes pointer
			assert.Equal(t, tt.exp, expType.Indirect(expZeroVal))
		})
	}
}

func TestJSONCodec_GetContentType(t *testing.T) {
	tests := []struct {
		name  string
		codec streams.Codec
		exp   string
	}{
		{
			name:  "basic",
			codec: streams.JSONCodec{},
			exp:   "application/json",
		},
		{
			name:  "buffered",
			codec: streams.NewJSONCodecBuffered(0, 0),
			exp:   "application/json",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.exp, tt.codec.GetContentType())
		})
	}
}

func BenchmarkJSONCodec_Marshal(b *testing.B) {
	// avoid b.Run here as it will run bench cases concurrently, causing race conditions.
	var globalEncoder streams.Encoder = streams.JSONCodec{}
	in := fakeMessage{
		ID:             "123",
		CorrelationID:  "123",
		CausationID:    "123",
		StreamName:     "neutrino.places.foo.raw",
		StreamVersion:  2,
		StreamFullName: "neutrino.places.foo.raw.v2",
		SchemaURL:      "https://events.docs.neutrinocorp.org/places#foo_raw",
		Data:           []byte("this is a test 0"),
		Metadata: map[string]string{
			"example": "lorem ipsum",
		},
	}
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_, _ = globalEncoder.Marshal(in)
	}
}

func BenchmarkJSONCodecBuffered_Marshal(b *testing.B) {
	var globalEncoder streams.Encoder = streams.NewJSONCodecBuffered(0, 0)
	in := fakeMessage{
		ID:             "123",
		CorrelationID:  "123",
		CausationID:    "123",
		StreamName:     "neutrino.places.foo.raw",
		StreamVersion:  2,
		StreamFullName: "neutrino.places.foo.raw.v2",
		SchemaURL:      "https://events.docs.neutrinocorp.org/places#foo_raw",
		Data:           []byte("this is a test 0"),
		Metadata: map[string]string{
			"example": "lorem ipsum",
		},
	}
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_, _ = globalEncoder.Marshal(in)
	}
}

func BenchmarkJSONCodec_Unmarshal(b *testing.B) {
	var globalDecoder streams.Decoder = streams.JSONCodec{}
	encodedMsg := []byte("{\"message_id\":\"123\",\"correlation_id\":\"123\",\"causation_id\":\"123\",\"stream_name\":\"neutrino.places.foo.raw\",\"stream_version\":2,\"stream_full_name\":\"neutrino.places.foo.raw.v2\",\"schema_url\":\"https://events.docs.neutrinocorp.org/places#foo_raw\",\"data\":\"dGhpcyBpcyBhIHRlc3QgMA==\",\"metadata\":{\"example\":\"lorem ipsum\"}}")
	msg := new(fakeMessage)
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = globalDecoder.Unmarshal(encodedMsg, msg)
	}
}

func BenchmarkJSONCodecBuffered_Unmarshal(b *testing.B) {
	var globalDecoder streams.Decoder = streams.NewJSONCodecBuffered(0, 0)
	encodedMsg := []byte("{\"message_id\":\"123\",\"correlation_id\":\"123\",\"causation_id\":\"123\",\"stream_name\":\"neutrino.places.foo.raw\",\"stream_version\":2,\"stream_full_name\":\"neutrino.places.foo.raw.v2\",\"schema_url\":\"https://events.docs.neutrinocorp.org/places#foo_raw\",\"data\":\"dGhpcyBpcyBhIHRlc3QgMA==\",\"metadata\":{\"example\":\"lorem ipsum\"}}")
	msg := new(fakeMessage)
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = globalDecoder.Unmarshal(encodedMsg, msg)
	}
}

func BenchmarkJSONCodecBuffered_MarshalMany(b *testing.B) {
	var encoder streams.MultiEncoder = streams.NewJSONCodecBuffered(0, 0)
	in := []interface{}{
		fakeMessage{
			ID:             "123",
			CorrelationID:  "123",
			CausationID:    "123",
			StreamName:     "neutrino.places.foo.raw",
			StreamVersion:  2,
			StreamFullName: "neutrino.places.foo.raw.v2",
			SchemaURL:      "https://events.docs.neutrinocorp.org/places#foo_raw",
			Data:           []byte("this is a test 0"),
			Metadata: map[string]string{
				"example": "lorem ipsum",
			},
		},
		fakeMessage{
			ID:             "456",
			CorrelationID:  "123",
			CausationID:    "123",
			StreamName:     "neutrino.places.foo.raw",
			StreamVersion:  2,
			StreamFullName: "neutrino.places.foo.raw.v2",
			SchemaURL:      "https://events.docs.neutrinocorp.org/places#foo_raw",
			Data:           []byte("this is a test 1"),
			Metadata:       nil,
		},
		fakeMessage{
			ID:             "789",
			CorrelationID:  "123",
			CausationID:    "456",
			StreamName:     "neutrino.places.foo.raw",
			StreamVersion:  2,
			StreamFullName: "neutrino.places.foo.raw.v2",
			SchemaURL:      "https://events.docs.neutrinocorp.org/places#foo_raw",
			Data:           []byte("this is a test 2"),
			Metadata:       nil,
		},
	}
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_, _ = encoder.MarshalMany(in)
	}
}

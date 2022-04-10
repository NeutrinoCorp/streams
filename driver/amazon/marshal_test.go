package amazon_test

import (
	"testing"

	"github.com/neutrinocorp/streamhub"
	"github.com/neutrinocorp/streamhub/driver/amazon"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMarshalSnsMessage(t *testing.T) {
	tests := []struct {
		Name string
		In   streamhub.Message
		Exp  string
		Err  error
	}{
		{
			Name: "Empty",
			In:   streamhub.Message{},
			Exp:  "{\"id\":\"\",\"stream\":\"\",\"source\":\"\",\"specversion\":\"\",\"type\":\"\",\"data\":null,\"correlation_id\":\"\",\"causation_id\":\"\"}",
			Err:  nil,
		},
		{
			Name: "Populated",
			In: streamhub.Message{
				ID:     "123",
				Source: "org.ncorp.foo",
				Stream: "foo.bar.baz",
				Data:   []byte("lorem ipsum dolor sit amet"),
			},
			Exp: "{\"id\":\"123\",\"stream\":\"foo.bar.baz\",\"source\":\"org.ncorp.foo\",\"specversion\":\"\",\"type\":\"\",\"data\":\"bG9yZW0gaXBzdW0gZG9sb3Igc2l0IGFtZXQ=\",\"correlation_id\":\"\",\"causation_id\":\"\"}",
			Err: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.Name, func(t *testing.T) {
			exp, err := amazon.MarshalMessage(tt.In)
			assert.Equal(t, tt.Err, err)
			if err == nil {
				require.NotNil(t, exp)
				assert.Equal(t, tt.Exp, *exp)
			}
		})
	}
}

func BenchmarkMarshalSnsMessage(b *testing.B) {
	inMsg := streamhub.Message{
		ID:     "123",
		Source: "org.ncorp.foo",
		Stream: "foo.bar.baz",
		Data:   []byte("lorem ipsum dolor sit amet"),
	}
	for i := 0; i < b.N; i++ {
		b.ReportAllocs()
		_, _ = amazon.MarshalMessage(inMsg)
	}
}

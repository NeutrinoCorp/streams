package streamhub_sarama

import (
	"testing"
	"time"

	"github.com/Shopify/sarama"
	"github.com/neutrinocorp/streamhub"
	"github.com/stretchr/testify/assert"
)

var serializerKHeadersTestSuite = []struct {
	In  streamhub.Message
	Out []sarama.RecordHeader
}{
	{
		In:  streamhub.Message{},
		Out: nil,
	},
	{
		In: streamhub.Message{
			ID: "123",
		},
		Out: []sarama.RecordHeader{
			{
				Key:   []byte(headerKeyCloudEventId),
				Value: []byte("123"),
			},
		},
	},
	{
		In: streamhub.Message{
			ID:     "123",
			Stream: "foo",
			Source: "foo-source",
		},
		Out: []sarama.RecordHeader{
			{
				Key:   []byte(headerKeyCloudEventId),
				Value: []byte("123"),
			},
			{
				Key:   []byte(headerKeyCloudEventSource),
				Value: []byte("foo-source"),
			},
			{
				Key:   []byte(headerKeyStreamhubStream),
				Value: []byte("foo"),
			},
		},
	},
	{
		In: streamhub.Message{
			ID:                "123",
			Stream:            "foo-stream",
			Source:            "foo-source",
			SpecVersion:       streamhub.CloudEventsSpecVersion,
			Type:              "foo-type",
			Data:              []byte("foo"),
			DataContentType:   "application/json",
			DataSchema:        "foo-schema",
			DataSchemaVersion: 0,
			Timestamp:         "",
			CorrelationID:     "123",
			CausationID:       "abc",
			DecodedData:       nil,
			GroupName:         "foo",
		},
		Out: []sarama.RecordHeader{
			{
				Key:   []byte(headerKeyCloudEventId),
				Value: []byte("123"),
			},
			{
				Key:   []byte(headerKeyCloudEventSource),
				Value: []byte("foo-source"),
			},
			{
				Key:   []byte(headerKeyCloudEventSpecVersion),
				Value: []byte(streamhub.CloudEventsSpecVersion),
			},
			{
				Key:   []byte(headerKeyCloudEventType),
				Value: []byte("foo-type"),
			},
			{
				Key:   []byte(headerKeyCloudEventSchema),
				Value: []byte("foo-schema"),
			},
			{
				Key:   []byte(headerKeyStreamhubStream),
				Value: []byte("foo-stream"),
			},
			{
				Key:   []byte(headerKeyStreamhubCorrelationId),
				Value: []byte("123"),
			},
			{
				Key:   []byte(headerKeyStreamhubCausationId),
				Value: []byte("abc"),
			},
			{
				Key:   []byte(headerKeyContentType),
				Value: []byte("application/json"),
			},
		},
	},
	{
		In: streamhub.Message{
			ID:                "123",
			Stream:            "foo-stream",
			Source:            "foo-source",
			SpecVersion:       streamhub.CloudEventsSpecVersion,
			Type:              "foo-type",
			Data:              []byte("foo"),
			DataContentType:   "application/json",
			DataSchema:        "foo-schema",
			DataSchemaVersion: 0,
			Timestamp:         "123456789",
			CorrelationID:     "123",
			CausationID:       "abc",
			DecodedData:       nil,
			GroupName:         "foo",
		},
		Out: []sarama.RecordHeader{
			{
				Key:   []byte(headerKeyCloudEventId),
				Value: []byte("123"),
			},
			{
				Key:   []byte(headerKeyCloudEventSource),
				Value: []byte("foo-source"),
			},
			{
				Key:   []byte(headerKeyCloudEventSpecVersion),
				Value: []byte(streamhub.CloudEventsSpecVersion),
			},
			{
				Key:   []byte(headerKeyCloudEventType),
				Value: []byte("foo-type"),
			},
			{
				Key:   []byte(headerKeyCloudEventSchema),
				Value: []byte("foo-schema"),
			},
			{
				Key:   []byte(headerKeyCloudEventTime),
				Value: []byte("123456789"),
			},
			{
				Key:   []byte(headerKeyStreamhubStream),
				Value: []byte("foo-stream"),
			},
			{
				Key:   []byte(headerKeyStreamhubCorrelationId),
				Value: []byte("123"),
			},
			{
				Key:   []byte(headerKeyStreamhubCausationId),
				Value: []byte("abc"),
			},
			{
				Key:   []byte(headerKeyContentType),
				Value: []byte("application/json"),
			},
		},
	},
	{
		In: streamhub.Message{
			ID:                "123",
			Stream:            "foo-stream",
			Source:            "foo-source",
			SpecVersion:       streamhub.CloudEventsSpecVersion,
			Type:              "foo-type",
			Data:              []byte("foo"),
			DataContentType:   "application/json",
			DataSchema:        "foo-schema",
			DataSchemaVersion: -1,
			Timestamp:         "-1",
			CorrelationID:     "123",
			CausationID:       "abc",
			DecodedData:       nil,
			GroupName:         "foo",
		},
		Out: []sarama.RecordHeader{
			{
				Key:   []byte(headerKeyCloudEventId),
				Value: []byte("123"),
			},
			{
				Key:   []byte(headerKeyCloudEventSource),
				Value: []byte("foo-source"),
			},
			{
				Key:   []byte(headerKeyCloudEventSpecVersion),
				Value: []byte(streamhub.CloudEventsSpecVersion),
			},
			{
				Key:   []byte(headerKeyCloudEventType),
				Value: []byte("foo-type"),
			},
			{
				Key:   []byte(headerKeyCloudEventSchema),
				Value: []byte("foo-schema"),
			},
			{
				Key:   []byte(headerKeyCloudEventTime),
				Value: []byte("-1"),
			},
			{
				Key:   []byte(headerKeyStreamhubStream),
				Value: []byte("foo-stream"),
			},
			{
				Key:   []byte(headerKeyStreamhubCorrelationId),
				Value: []byte("123"),
			},
			{
				Key:   []byte(headerKeyStreamhubCausationId),
				Value: []byte("abc"),
			},
			{
				Key:   []byte(headerKeyContentType),
				Value: []byte("application/json"),
			},
		},
	},
	{
		In: streamhub.Message{
			ID:                "123",
			Stream:            "foo-stream",
			Source:            "foo-source",
			SpecVersion:       streamhub.CloudEventsSpecVersion,
			Type:              "foo-type",
			Data:              []byte("foo"),
			DataContentType:   "application/json",
			DataSchema:        "foo-schema",
			DataSchemaVersion: 1,
			Timestamp:         "123456789",
			CorrelationID:     "123",
			CausationID:       "abc",
			DecodedData:       nil,
			GroupName:         "foo",
		},
		Out: []sarama.RecordHeader{
			{
				Key:   []byte(headerKeyCloudEventId),
				Value: []byte("123"),
			},
			{
				Key:   []byte(headerKeyCloudEventSource),
				Value: []byte("foo-source"),
			},
			{
				Key:   []byte(headerKeyCloudEventSpecVersion),
				Value: []byte(streamhub.CloudEventsSpecVersion),
			},
			{
				Key:   []byte(headerKeyCloudEventType),
				Value: []byte("foo-type"),
			},
			{
				Key:   []byte(headerKeyCloudEventSchema),
				Value: []byte("foo-schema"),
			},
			{
				Key:   []byte(headerKeyCloudEventSchemaVersion),
				Value: []byte("1"),
			},
			{
				Key:   []byte(headerKeyCloudEventTime),
				Value: []byte("123456789"),
			},
			{
				Key:   []byte(headerKeyStreamhubStream),
				Value: []byte("foo-stream"),
			},
			{
				Key:   []byte(headerKeyStreamhubCorrelationId),
				Value: []byte("123"),
			},
			{
				Key:   []byte(headerKeyStreamhubCausationId),
				Value: []byte("abc"),
			},
			{
				Key:   []byte(headerKeyContentType),
				Value: []byte("application/json"),
			},
		},
	},
	{
		In: streamhub.Message{
			ID:                "123",
			Stream:            "foo-stream",
			Source:            "foo-source",
			SpecVersion:       streamhub.CloudEventsSpecVersion,
			Type:              "foo-type",
			Data:              []byte("foo"),
			DataContentType:   "application/json",
			DataSchema:        "foo-schema",
			DataSchemaVersion: 1,
			Timestamp:         "123456789",
			Subject:           "foo-subject",
			CorrelationID:     "123",
			CausationID:       "abc",
			DecodedData:       nil,
			GroupName:         "foo",
		},
		Out: []sarama.RecordHeader{
			{
				Key:   []byte(headerKeyCloudEventId),
				Value: []byte("123"),
			},
			{
				Key:   []byte(headerKeyCloudEventSource),
				Value: []byte("foo-source"),
			},
			{
				Key:   []byte(headerKeyCloudEventSpecVersion),
				Value: []byte(streamhub.CloudEventsSpecVersion),
			},
			{
				Key:   []byte(headerKeyCloudEventType),
				Value: []byte("foo-type"),
			},
			{
				Key:   []byte(headerKeyCloudEventSchema),
				Value: []byte("foo-schema"),
			},
			{
				Key:   []byte(headerKeyCloudEventSchemaVersion),
				Value: []byte("1"),
			},
			{
				Key:   []byte(headerKeyCloudEventTime),
				Value: []byte("123456789"),
			},
			{
				Key:   []byte(headerKeyCloudEventSubject),
				Value: []byte("foo-subject"),
			},
			{
				Key:   []byte(headerKeyStreamhubStream),
				Value: []byte("foo-stream"),
			},
			{
				Key:   []byte(headerKeyStreamhubCorrelationId),
				Value: []byte("123"),
			},
			{
				Key:   []byte(headerKeyStreamhubCausationId),
				Value: []byte("abc"),
			},
			{
				Key:   []byte(headerKeyContentType),
				Value: []byte("application/json"),
			},
		},
	},
}

func TestSerializeKHeaders(t *testing.T) {
	for _, tt := range serializerKHeadersTestSuite {
		t.Run("", func(t *testing.T) {
			out := serializeKHeaders(tt.In)
			assert.Equal(t, tt.Out, out)
		})
	}
}

func BenchmarkSerializeKHeaders(b *testing.B) {
	msg := streamhub.Message{
		ID:                "123",
		Stream:            "foo-stream",
		Source:            "foo-source",
		SpecVersion:       streamhub.CloudEventsSpecVersion,
		Type:              "foo-type",
		Data:              []byte("foo"),
		DataContentType:   "application/json",
		DataSchema:        "foo-schema",
		DataSchemaVersion: 1,
		Timestamp:         time.Now().UTC().Format(time.RFC3339),
		Subject:           "foo-sub",
		CorrelationID:     "123",
		CausationID:       "123",
		DecodedData:       nil,
		GroupName:         "foo",
	}
	for i := 0; i < b.N; i++ {
		b.ReportAllocs()
		_ = serializeKHeaders(msg)
	}
}

var deserializerKHeadersTestSuite = []struct {
	In  []*sarama.RecordHeader
	Out streamhub.Message
}{
	{
		In:  nil,
		Out: streamhub.Message{},
	},
	{
		In: []*sarama.RecordHeader{
			{
				Key:   []byte(headerKeyCloudEventId),
				Value: []byte("123"),
			},
		},
		Out: streamhub.Message{
			ID: "123",
		},
	},
	{
		In: []*sarama.RecordHeader{
			{
				Key:   []byte(headerKeyCloudEventId),
				Value: []byte("123"),
			},
		},
		Out: streamhub.Message{
			ID:                "123",
			DataSchemaVersion: 0,
		},
	},
	{
		In: []*sarama.RecordHeader{
			{
				Key:   []byte(headerKeyCloudEventId),
				Value: []byte("123"),
			},
		},
		Out: streamhub.Message{
			ID: "123",
		},
	},
	{
		In: []*sarama.RecordHeader{
			{
				Key:   []byte(headerKeyCloudEventId),
				Value: []byte("123"),
			},
			{
				Key:   []byte(headerKeyCloudEventSchemaVersion),
				Value: []byte("2"),
			},
		},
		Out: streamhub.Message{
			ID:                "123",
			DataSchemaVersion: 2,
		},
	},
	{
		In: []*sarama.RecordHeader{
			{
				Key:   []byte(headerKeyCloudEventId),
				Value: []byte("123"),
			},
			{
				Key:   []byte(headerKeyCloudEventSource),
				Value: []byte("foo-source"),
			},
			{
				Key:   []byte(headerKeyCloudEventSpecVersion),
				Value: []byte(streamhub.CloudEventsSpecVersion),
			},
			{
				Key:   []byte(headerKeyCloudEventType),
				Value: []byte("foo-type"),
			},
			{
				Key:   []byte(headerKeyCloudEventSchema),
				Value: []byte("foo-schema"),
			},
			{
				Key:   []byte(headerKeyCloudEventSchemaVersion),
				Value: []byte("1"),
			},
			{
				Key:   []byte(headerKeyCloudEventTime),
				Value: []byte("123456789"),
			},
			{
				Key:   []byte(headerKeyCloudEventSubject),
				Value: []byte("foo-subject"),
			},
			{
				Key:   []byte(headerKeyStreamhubStream),
				Value: []byte("foo-stream"),
			},
			{
				Key:   []byte(headerKeyStreamhubCorrelationId),
				Value: []byte("123"),
			},
			{
				Key:   []byte(headerKeyStreamhubCausationId),
				Value: []byte("abc"),
			},
			{
				Key:   []byte(headerKeyContentType),
				Value: []byte("application/json"),
			},
			{
				Key:   []byte("ce_data"), // non-valid
				Value: []byte("hello world"),
			},
			{
				Key:   []byte("sh_group"), // non-valid
				Value: []byte("foo-group"),
			},
		},
		Out: streamhub.Message{
			ID:                "123",
			Stream:            "foo-stream",
			Source:            "foo-source",
			SpecVersion:       streamhub.CloudEventsSpecVersion,
			Type:              "foo-type",
			Data:              nil,
			DataContentType:   "application/json",
			DataSchema:        "foo-schema",
			DataSchemaVersion: 1,
			Timestamp:         "123456789",
			Subject:           "foo-subject",
			CorrelationID:     "123",
			CausationID:       "abc",
			DecodedData:       nil,
			GroupName:         "",
		},
	},
}

func TestDeserializeKHeaders(t *testing.T) {
	for _, tt := range deserializerKHeadersTestSuite {
		t.Run("", func(t *testing.T) {
			out := deserializeKHeaders(tt.In)
			assert.Equal(t, tt.Out, out)
		})
	}
}

func BenchmarkDeserializeKHeaders(b *testing.B) {
	headers := []*sarama.RecordHeader{
		{
			Key:   []byte(headerKeyCloudEventId),
			Value: []byte("123"),
		},
		{
			Key:   []byte(headerKeyCloudEventSource),
			Value: []byte("foo-source"),
		},
		{
			Key:   []byte(headerKeyCloudEventSpecVersion),
			Value: []byte(streamhub.CloudEventsSpecVersion),
		},
		{
			Key:   []byte(headerKeyCloudEventType),
			Value: []byte("foo-type"),
		},
		{
			Key:   []byte(headerKeyCloudEventSchema),
			Value: []byte("foo-schema"),
		},
		{
			Key:   []byte(headerKeyCloudEventSchemaVersion),
			Value: []byte("1"),
		},
		{
			Key:   []byte(headerKeyCloudEventTime),
			Value: []byte("123456789"),
		},
		{
			Key:   []byte(headerKeyCloudEventSubject),
			Value: []byte("foo-subject"),
		},
		{
			Key:   []byte(headerKeyStreamhubStream),
			Value: []byte("foo-stream"),
		},
		{
			Key:   []byte(headerKeyStreamhubCorrelationId),
			Value: []byte("123"),
		},
		{
			Key:   []byte(headerKeyStreamhubCausationId),
			Value: []byte("abc"),
		},
		{
			Key:   []byte(headerKeyContentType),
			Value: []byte("application/json"),
		},
	}
	for i := 0; i < b.N; i++ {
		b.ReportAllocs()
		_ = deserializeKHeaders(headers)
	}
}

var defaultTimestamp = time.Now()

var consumerToProducerMessageTestSuite = []struct {
	In  *sarama.ConsumerMessage
	Out *sarama.ProducerMessage
}{
	{
		In:  nil,
		Out: nil,
	},
	{
		In: &sarama.ConsumerMessage{
			Headers:        nil,
			Timestamp:      time.Time{},
			BlockTimestamp: time.Time{},
			Key:            nil,
			Value:          nil,
			Topic:          "",
			Partition:      0,
			Offset:         0,
		},
		Out: &sarama.ProducerMessage{
			Topic:     "",
			Key:       nil,
			Value:     nil,
			Headers:   nil,
			Metadata:  nil,
			Offset:    0,
			Partition: 0,
			Timestamp: time.Time{},
		},
	},
	{
		In: &sarama.ConsumerMessage{
			Headers: []*sarama.RecordHeader{
				{
					Key:   []byte("foo"),
					Value: []byte("bar"),
				},
			},
			Timestamp:      defaultTimestamp,
			BlockTimestamp: time.Time{},
			Key:            []byte("abc"),
			Value:          []byte("foobar"),
			Topic:          "foo-topic",
			Partition:      10,
			Offset:         8,
		},
		Out: &sarama.ProducerMessage{
			Key:       sarama.ByteEncoder("abc"),
			Value:     sarama.ByteEncoder("foobar"),
			Topic:     "foo-topic",
			Partition: 10,
			Offset:    8,
			Headers: []sarama.RecordHeader{
				{
					Key:   []byte("foo"),
					Value: []byte("bar"),
				},
			},
			Metadata:  nil,
			Timestamp: defaultTimestamp,
		},
	},
}

func TestConsumerToProducerMessage(t *testing.T) {
	for _, tt := range consumerToProducerMessageTestSuite {
		t.Run("", func(t *testing.T) {
			out := consumerToProducerMessage(tt.In)
			assert.EqualValues(t, tt.Out, out)
		})
	}
}

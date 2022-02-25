package streamhub_sarama

import (
	"errors"
	"testing"
	"time"

	"github.com/Shopify/sarama"
	"github.com/neutrinocorp/streamhub"
	"github.com/stretchr/testify/assert"
)

type mockSyncPublisher struct {
	sendMsgHook func(messaged ...*sarama.ProducerMessage)
}

var _ sarama.SyncProducer = mockSyncPublisher{}

func (m mockSyncPublisher) SendMessage(msg *sarama.ProducerMessage) (partition int32, offset int64, err error) {
	if m.sendMsgHook != nil {
		m.sendMsgHook(msg)
	}
	return 0, 0, nil
}

func (m mockSyncPublisher) SendMessages(msgs []*sarama.ProducerMessage) error {
	if m.sendMsgHook != nil {
		m.sendMsgHook(msgs...)
	}
	return nil
}

func (m mockSyncPublisher) Close() error {
	return nil
}

var syncPublisherTests = []struct {
	In         PublisherStrategy
	InMarshall streamhub.Marshaler
	InMsg      streamhub.Message
	Exp        error
}{
	{
		In:         0,
		InMarshall: nil,
		InMsg:      streamhub.Message{},
		Exp:        nil,
	},
	{
		In:         PublisherRoundRobinStrategy,
		InMarshall: nil,
		InMsg:      streamhub.Message{},
		Exp:        nil,
	},
	{
		In:         PublisherSubjectStrategy,
		InMarshall: nil,
		InMsg: streamhub.Message{
			Subject: "foo-sub",
		},
		Exp: nil,
	},
	{
		In:         PublisherMessageIdStrategy,
		InMarshall: nil,
		InMsg: streamhub.Message{
			ID: "abc",
		},
		Exp: nil,
	},
	{
		In:         PublisherCorrelationIdStrategy,
		InMarshall: nil,
		InMsg: streamhub.Message{
			CorrelationID: "123f",
		},
		Exp: nil,
	},
	{
		In:         0,
		InMarshall: streamhub.FailingMarshalerNoop{},
		InMsg:      streamhub.Message{},
		Exp:        errors.New("failing marshal"),
	},
	{
		In:         0,
		InMarshall: streamhub.NewAvroMarshaler(),
		InMsg:      streamhub.Message{},
		Exp:        errors.New("avro: unknown type: "), // will fail as no schema registry was defined
	},
	{
		In:         0,
		InMarshall: streamhub.JSONMarshaler{},
		InMsg:      streamhub.Message{},
		Exp:        nil,
	},
}

func TestSyncPublisher_Publish(t *testing.T) {
	p := NewSyncPublisher(mockSyncPublisher{}, streamhub.JSONMarshaler{}, -1)
	assert.Equal(t, PublisherRoundRobinStrategy, p.Strategy)

	for _, tt := range syncPublisherTests {
		t.Run("", func(t *testing.T) {
			mockPublisher := mockSyncPublisher{sendMsgHook: func(messages ...*sarama.ProducerMessage) {
				for _, message := range messages {
					assert.Equal(t, tt.InMsg.Stream, message.Topic)
					if tt.InMsg.Timestamp == "" {
						assert.Empty(t, message.Timestamp)
					} else {
						assert.Equal(t, tt.InMsg.Timestamp, message.Timestamp.Format(time.RFC3339))
					}

					if tt.In == PublisherRoundRobinStrategy {
						assert.Nil(t, message.Key)
						return
					}
					buff, _ := message.Key.Encode()
					keyStr := string(buff)
					switch tt.In {
					case PublisherCorrelationIdStrategy:
						assert.Equal(t, tt.InMsg.CorrelationID, keyStr)
					case PublisherSubjectStrategy:
						assert.Equal(t, tt.InMsg.Subject, keyStr)
					case PublisherMessageIdStrategy:
						assert.Equal(t, tt.InMsg.ID, keyStr)
					}
				}
			}}
			p = NewSyncPublisher(mockPublisher, tt.InMarshall, tt.In)
			err := p.Publish(nil, tt.InMsg)
			assert.Equal(t, tt.Exp, err)
		})
	}
}

func TestSyncPublisher_PublishBatch(t *testing.T) {
	p := NewSyncPublisher(mockSyncPublisher{}, streamhub.JSONMarshaler{}, -1)
	assert.Equal(t, PublisherRoundRobinStrategy, p.Strategy)

	for _, tt := range syncPublisherTests {
		t.Run("", func(t *testing.T) {
			mockPublisher := mockSyncPublisher{sendMsgHook: func(messages ...*sarama.ProducerMessage) {
				assert.Equal(t, 2, len(messages))
				for _, message := range messages {
					assert.Equal(t, tt.InMsg.Stream, message.Topic)
					if tt.InMsg.Timestamp == "" {
						assert.Empty(t, message.Timestamp)
					} else {
						assert.Equal(t, tt.InMsg.Timestamp, message.Timestamp.Format(time.RFC3339))
					}

					if tt.In == PublisherRoundRobinStrategy {
						assert.Nil(t, message.Key)
						return
					}
					buff, _ := message.Key.Encode()
					keyStr := string(buff)
					switch tt.In {
					case PublisherCorrelationIdStrategy:
						assert.Equal(t, tt.InMsg.CorrelationID, keyStr)
					case PublisherSubjectStrategy:
						assert.Equal(t, tt.InMsg.Subject, keyStr)
					case PublisherMessageIdStrategy:
						assert.Equal(t, tt.InMsg.ID, keyStr)
					}
				}
			}}
			p = NewSyncPublisher(mockPublisher, tt.InMarshall, tt.In)
			err := p.PublishBatch(nil, tt.InMsg, tt.InMsg)
			assert.Equal(t, tt.Exp, err)
		})
	}
}

func BenchmarkSyncPublisher_Publish_Binary(b *testing.B) {
	p := NewSyncPublisher(mockSyncPublisher{}, nil, PublisherMessageIdStrategy)
	msg := streamhub.Message{
		ID: "abc1f",
	}
	for i := 0; i < b.N; i++ {
		b.ReportAllocs()
		_ = p.Publish(nil, msg)
	}
}

func BenchmarkSyncPublisher_Publish_Structured(b *testing.B) {
	p := NewSyncPublisher(mockSyncPublisher{}, streamhub.JSONMarshaler{}, PublisherMessageIdStrategy)
	msg := streamhub.Message{
		ID: "abc1f",
	}
	for i := 0; i < b.N; i++ {
		b.ReportAllocs()
		_ = p.Publish(nil, msg)
	}
}

func TestPopulateProducerMessage(t *testing.T) {
	var marshaler streamhub.Marshaler = streamhub.FailingMarshalerNoop{}
	msg := streamhub.Message{
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
	}
	producerMsg := new(sarama.ProducerMessage)
	err := populateProducerMessage(marshaler, msg, producerMsg)
	assert.Error(t, err)
	assert.Empty(t, producerMsg.Value)

	marshaler = streamhub.JSONMarshaler{}
	err = populateProducerMessage(marshaler, msg, producerMsg)
	assert.NoError(t, err)
	assert.NotEmpty(t, producerMsg.Value)

	err = populateProducerMessage(nil, msg, producerMsg)
	assert.NoError(t, err)
	assert.NotEmpty(t, producerMsg.Headers)
}

func BenchmarkPopulateProducerMessageBinary(b *testing.B) {
	// using headers
	msg := streamhub.Message{
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
	}
	producerMsg := new(sarama.ProducerMessage)
	for i := 0; i < b.N; i++ {
		b.ReportAllocs()
		_ = populateProducerMessage(nil, msg, producerMsg)
	}
}

func BenchmarkPopulateProducerMessageStructured(b *testing.B) {
	// compress as JSON into data field
	marshaler := streamhub.JSONMarshaler{}
	msg := streamhub.Message{
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
	}
	producerMsg := new(sarama.ProducerMessage)
	for i := 0; i < b.N; i++ {
		b.ReportAllocs()
		_ = populateProducerMessage(marshaler, msg, producerMsg)
	}
}

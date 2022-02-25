package streamhub_sarama

import (
	"strconv"

	"github.com/Shopify/sarama"
	"github.com/neutrinocorp/streamhub"
	"github.com/neutrinocorp/streamhub/parser"
)

const (
	headerKeyCloudEventId            = "ce_id"
	headerKeyCloudEventSource        = "ce_source"
	headerKeyCloudEventSpecVersion   = "ce_specversion"
	headerKeyCloudEventType          = "ce_type"
	headerKeyCloudEventSchema        = "ce_schema"
	headerKeyCloudEventSchemaVersion = "ce_schemaversion"
	headerKeyCloudEventTime          = "ce_time"
	headerKeyCloudEventSubject       = "ce_subject"
	headerKeyStreamhubStream         = "sh_stream"
	headerKeyStreamhubCorrelationId  = "sh_correlation_id"
	headerKeyStreamhubCausationId    = "sh_causation_id"
	headerKeyContentType             = "content-type"
)

// calculateTotalMessageFields returns the number of fields with non-zero values from a streamhub.Message.
//
// Useful to set up the cap of a sarama.RecordHeader slice and save space allocation.
//
// Moreover, it skips fields such as `Data` and Consumer-oriented fields in order to avoid false positives.
//
// Getting deeper, it is based on field static-checking as relying on built-in `reflect` package impacts serialization
// performance in a great badly manner.
// Hence, field dynamic-checking is not available so the function will ignore new fields added to streamhub.Message in
// later updates if this function was not updated.
func calculateTotalMessageFields(msg streamhub.Message) int {
	totalFields := 0
	if msg.ID != "" {
		totalFields++
	}
	if msg.Stream != "" {
		totalFields++
	}
	if msg.Source != "" {
		totalFields++
	}
	if msg.SpecVersion != "" {
		totalFields++
	}
	if msg.Type != "" {
		totalFields++
	}
	if msg.DataContentType != "" {
		totalFields++
	}
	if msg.DataSchema != "" {
		totalFields++
	}
	if msg.DataSchemaVersion > 0 {
		totalFields++
	}
	if msg.Timestamp != "" {
		totalFields++
	}
	if msg.Subject != "" {
		totalFields++
	}
	if msg.CorrelationID != "" {
		totalFields++
	}
	if msg.CausationID != "" {
		totalFields++
	}
	return totalFields
}

// newKHeader adds a new sarama.RecordHeader if key or value are present from a streamhub.Message field
//
// Thus, non-present fields are not written into the given sarama.RecordHeader slice
func appendKHeader(headers []sarama.RecordHeader, key, value string) []sarama.RecordHeader {
	if value == "" || key == "" || value == "0" {
		return headers
	}

	return append(headers, sarama.RecordHeader{
		Key:   parser.UnsafeStringToBytes(key),
		Value: parser.UnsafeStringToBytes(value),
	})
}

func parseValidInt(v int) string {
	if v <= 0 {
		return ""
	}
	return strconv.Itoa(v)
}

func serializeKHeaders(msg streamhub.Message) []sarama.RecordHeader {
	totalFields := calculateTotalMessageFields(msg)
	if totalFields == 0 {
		return nil
	}
	headers := make([]sarama.RecordHeader, 0, totalFields)

	headers = appendKHeader(headers, headerKeyCloudEventId, msg.ID)
	headers = appendKHeader(headers, headerKeyCloudEventSource, msg.Source)
	headers = appendKHeader(headers, headerKeyCloudEventSpecVersion, msg.SpecVersion)
	headers = appendKHeader(headers, headerKeyCloudEventType, msg.Type)
	headers = appendKHeader(headers, headerKeyCloudEventSchema, msg.DataSchema)
	headers = appendKHeader(headers, headerKeyCloudEventSchemaVersion, parseValidInt(msg.DataSchemaVersion))
	headers = appendKHeader(headers, headerKeyCloudEventTime, msg.Timestamp)
	headers = appendKHeader(headers, headerKeyCloudEventSubject, msg.Subject)
	headers = appendKHeader(headers, headerKeyStreamhubStream, msg.Stream)
	headers = appendKHeader(headers, headerKeyStreamhubCorrelationId, msg.CorrelationID)
	headers = appendKHeader(headers, headerKeyStreamhubCausationId, msg.CausationID)
	headers = appendKHeader(headers, headerKeyContentType, msg.DataContentType)
	return headers
}

func deserializeKHeaders(headers []sarama.RecordHeader) streamhub.Message {
	msg := streamhub.Message{}
	for _, h := range headers {
		switch parser.UnsafeBytesToString(h.Key) {
		case headerKeyCloudEventId:
			msg.ID = parser.UnsafeBytesToString(h.Value)
		case headerKeyStreamhubStream:
			msg.Stream = parser.UnsafeBytesToString(h.Value)
		case headerKeyCloudEventSource:
			msg.Source = parser.UnsafeBytesToString(h.Value)
		case headerKeyCloudEventSpecVersion:
			msg.SpecVersion = parser.UnsafeBytesToString(h.Value)
		case headerKeyCloudEventType:
			msg.Type = parser.UnsafeBytesToString(h.Value)
		case headerKeyContentType:
			msg.DataContentType = parser.UnsafeBytesToString(h.Value)
		case headerKeyCloudEventSchema:
			msg.DataSchema = parser.UnsafeBytesToString(h.Value)
		case headerKeyCloudEventSchemaVersion:
			version, _ := strconv.Atoi(parser.UnsafeBytesToString(h.Value))
			msg.DataSchemaVersion = version
		case headerKeyCloudEventTime:
			msg.Timestamp = parser.UnsafeBytesToString(h.Value)
		case headerKeyCloudEventSubject:
			msg.Subject = parser.UnsafeBytesToString(h.Value)
		case headerKeyStreamhubCorrelationId:
			msg.CorrelationID = parser.UnsafeBytesToString(h.Value)
		case headerKeyStreamhubCausationId:
			msg.CausationID = parser.UnsafeBytesToString(h.Value)
		}
	}
	return msg
}

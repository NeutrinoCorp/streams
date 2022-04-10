package amazon

import (
	"github.com/aws/aws-sdk-go-v2/aws"
	jsoniter "github.com/json-iterator/go"
	"github.com/neutrinocorp/streamhub"
	"github.com/neutrinocorp/streamhub/parser"
)

// MarshalMessage converts a streamhub.Message into a JSON string ready to be published to Amazon Simple
// Notification Service (SNS) and/or Amazon Simple Queue Service (SQS).
func MarshalMessage(message streamhub.Message) (*string, error) {
	msgJSON, err := jsoniter.Marshal(message)
	if err != nil {
		return nil, err
	}
	return aws.String(parser.UnsafeBytesToString(msgJSON)), nil
}

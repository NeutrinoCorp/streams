package amazon

import (
	"strings"
	"sync"

	"github.com/aws/aws-sdk-go-v2/aws"
)

const (
	baseSnsTopicPrefix      = "arn:aws:sns:"
	snsTopicTotalSeparators = 2
)

var (
	nameSanitizer     *strings.Replacer
	onceNameSanitizer sync.Once
)

func init() {
	onceNameSanitizer.Do(func() {
		nameSanitizer = strings.NewReplacer(".", "-")
	})
}

// NewTopic builds an Amazon Simple Notification Service topic.
func NewTopic(region, accountID, baseTopic string) *string {
	isInvalid := accountID == "" || region == "" || baseTopic == ""
	if isInvalid {
		return nil
	}

	// Nomenclature: arn:aws:sns:REGION:ACCOUNT_ID:TOPIC
	bufferSize := len(baseSnsTopicPrefix) + len(region) + len(accountID) + len(baseTopic) +
		snsTopicTotalSeparators
	buff := strings.Builder{}
	buff.Grow(bufferSize)
	buff.WriteString(baseSnsTopicPrefix)
	buff.WriteString(region)
	buff.WriteString(":")
	buff.WriteString(accountID)
	buff.WriteString(":")
	buff.WriteString(nameSanitizer.Replace(baseTopic))
	return aws.String(buff.String())
}

const (
	sqsPrefix            = "https://sqs."
	sqsDomain            = ".amazonaws.com/"
	sqsTotalExtraSlashes = 1
)

// NewQueueUrl builds an Amazon Simple Queue Service's Queue URL.
func NewQueueUrl(region, accountID, queueName string) *string {
	isInvalid := accountID == "" || region == "" || queueName == ""
	if isInvalid {
		return nil
	}

	// Nomenclature: https://sqs.REGION.amazonaws.com/ACCOUNT_ID/QUEUE_NAME
	bufferSize := len(sqsPrefix) + len(region) + len(sqsDomain) + len(accountID) +
		sqsTotalExtraSlashes + len(queueName)
	buff := strings.Builder{}
	buff.Grow(bufferSize)
	buff.WriteString(sqsPrefix)
	buff.WriteString(region)
	buff.WriteString(sqsDomain)
	buff.WriteString(accountID)
	buff.WriteString("/")
	buff.WriteString(nameSanitizer.Replace(queueName))
	return aws.String(buff.String())
}

const (
	baseEventBridgeBusPrefix      = "arn:aws:events:"
	baseEventBridgeBusResource    = ":event-bus/"
	eventBridgeBusTotalSeparators = 1
)

// NewEventBusArn builds an Amazon EventBridge's Event Bus Amazon Resource Name (ARN).
func NewEventBusArn(region, accountID, baseBusName string) *string {
	isInvalid := region == "" || accountID == "" || baseBusName == ""
	if isInvalid {
		return nil
	}

	buffSize := len(baseEventBridgeBusPrefix) + len(region) + len(accountID) +
		len(baseEventBridgeBusResource) + len(baseBusName) + eventBridgeBusTotalSeparators
	buff := strings.Builder{}
	buff.Grow(buffSize)
	buff.WriteString(baseEventBridgeBusPrefix)
	buff.WriteString(region)
	buff.WriteString(":")
	buff.WriteString(accountID)
	buff.WriteString(baseEventBridgeBusResource)
	buff.WriteString(baseBusName)
	return aws.String(buff.String())
}

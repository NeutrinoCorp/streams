package amazon_test

import (
	"testing"

	"github.com/neutrinocorp/streamhub/driver/amazon"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/stretchr/testify/assert"
)

func TestNewTopic(t *testing.T) {
	tests := []struct {
		Name        string
		InAccountID string
		InRegion    string
		InTopic     string
		Exp         *string
	}{
		{
			Name:        "Empty",
			InAccountID: "",
			InRegion:    "",
			InTopic:     "",
			Exp:         nil,
		},
		{
			Name:        "Valid Broker Account ID",
			InAccountID: "123456789012",
			InRegion:    "",
			InTopic:     "",
			Exp:         nil,
		},
		{
			Name:        "Valid Amazon Region",
			InAccountID: "123456789012",
			InRegion:    "us-east-2",
			InTopic:     "",
			Exp:         nil,
		},
		{
			Name:        "Valid",
			InAccountID: "123456789012",
			InRegion:    "us-east-2",
			InTopic:     "ncorp.prod.platform.foo_bar.lorem",
			Exp:         aws.String("arn:aws:sns:us-east-2:123456789012:ncorp-prod-platform-foo_bar-lorem"),
		},
	}

	for _, tt := range tests {
		t.Run(tt.Name, func(t *testing.T) {
			exp := amazon.NewTopic(tt.InRegion, tt.InAccountID, tt.InTopic)
			assert.EqualValues(t, tt.Exp, exp)
		})
	}
}

func BenchmarkNewTopic(b *testing.B) {
	for i := 0; i < b.N; i++ {
		b.ReportAllocs()
		_ = amazon.NewTopic("us-east-2", "123456789012", "ncorp.prod.platform.foo_bar.lorem")
	}
}

func TestNewQueueUrl(t *testing.T) {
	tests := []struct {
		Name        string
		InAccountID string
		InRegion    string
		InQueueName string
		Exp         *string
	}{
		{
			Name:        "Empty",
			InAccountID: "",
			InRegion:    "",
			InQueueName: "",
			Exp:         nil,
		},
		{
			Name:        "Valid Broker Account ID",
			InAccountID: "123456789012",
			InRegion:    "",
			InQueueName: "",
			Exp:         nil,
		},
		{
			Name:        "Valid Amazon Region",
			InAccountID: "123456789012",
			InRegion:    "us-east-2",
			InQueueName: "",
			Exp:         nil,
		},
		{
			Name:        "Valid",
			InAccountID: "123456789012",
			InRegion:    "us-west-1",
			InQueueName: "ncorp.dev.platform.foo_bar.notify_user.on.bar.activated",
			Exp:         aws.String("https://sqs.us-west-1.amazonaws.com/123456789012/ncorp-dev-platform-foo_bar-notify_user-on-bar-activated"),
		},
	}

	for _, tt := range tests {
		t.Run(tt.Name, func(t *testing.T) {
			exp := amazon.NewQueueUrl(tt.InRegion, tt.InAccountID, tt.InQueueName)
			assert.EqualValues(t, tt.Exp, exp)
		})
	}
}

func BenchmarkNewQueueUrl(b *testing.B) {
	for i := 0; i < b.N; i++ {
		b.ReportAllocs()
		_ = amazon.NewQueueUrl("us-east-2", "123456789012", "ncorp.dev.platform.foo_bar.notify_user.on.bar.activated")
	}
}

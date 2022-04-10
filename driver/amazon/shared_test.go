package amazon_test

import (
	"context"
	"sync"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsCfg "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
)

type sampleEventFoo struct {
	Foo string `json:"foo"`
	Bar string `json:"bar"`
}

var (
	defaultLocalAwsConfig    aws.Config
	defaultLocalAwsAccountID = "000000000000"
	onceLocalAwsCfg          sync.Once
)

func init() {
	onceLocalAwsCfg.Do(func() {
		defaultLocalAwsConfig, _ = newLocalAwsConfig()
	})
}

func newLocalAwsConfig() (aws.Config, error) {
	// using localstack
	// took from: https://docs.localstack.cloud/integrations/sdks/go/
	awsEndpoint := "http://localhost:4566"
	awsRegion := "us-east-1"

	customResolver := aws.EndpointResolverWithOptionsFunc(func(service, region string, _ ...interface{}) (aws.Endpoint, error) {
		if awsEndpoint != "" {
			return aws.Endpoint{
				PartitionID:   "aws",
				URL:           awsEndpoint,
				SigningRegion: awsRegion,
			}, nil
		}

		// returning EndpointNotFoundError will allow the service to fall back to its default resolution
		return aws.Endpoint{}, &aws.EndpointNotFoundError{}
	})

	return awsCfg.LoadDefaultConfig(context.TODO(),
		awsCfg.WithRegion(awsRegion),
		awsCfg.WithEndpointResolverWithOptions(customResolver),
		awsCfg.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("DUMMY", "SECRET", "TOKEN")),
	)
}

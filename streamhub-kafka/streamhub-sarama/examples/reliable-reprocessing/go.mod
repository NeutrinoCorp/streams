module reliable-reprocessing

go 1.17

require (
	github.com/Shopify/sarama v1.31.1
	github.com/neutrinocorp/streamhub v0.0.0-20220210073221-ddb1aaf3d445
	github.com/neutrinocorp/streamhub/streamhub-sarama v0.0.0-20220210073221-ddb1aaf3d445
)

require (
	github.com/cenkalti/backoff/v4 v4.1.2 // indirect
	github.com/davecgh/go-spew v1.1.1 // indirect
	github.com/eapache/go-resiliency v1.2.0 // indirect
	github.com/eapache/go-xerial-snappy v0.0.0-20180814174437-776d5712da21 // indirect
	github.com/eapache/queue v1.1.0 // indirect
	github.com/golang/snappy v0.0.4 // indirect
	github.com/google/uuid v1.3.0 // indirect
	github.com/hamba/avro v1.6.3 // indirect
	github.com/hashicorp/go-uuid v1.0.2 // indirect
	github.com/hashicorp/golang-lru v0.5.4 // indirect
	github.com/jcmturner/aescts/v2 v2.0.0 // indirect
	github.com/jcmturner/dnsutils/v2 v2.0.0 // indirect
	github.com/jcmturner/gofork v1.0.0 // indirect
	github.com/jcmturner/gokrb5/v8 v8.4.2 // indirect
	github.com/jcmturner/rpc/v2 v2.0.3 // indirect
	github.com/json-iterator/go v1.1.12 // indirect
	github.com/klauspost/compress v1.14.2 // indirect
	github.com/modern-go/concurrent v0.0.0-20180306012644-bacd9c7ef1dd // indirect
	github.com/modern-go/reflect2 v1.0.2 // indirect
	github.com/pierrec/lz4 v2.6.1+incompatible // indirect
	github.com/rcrowley/go-metrics v0.0.0-20201227073835-cf1acfcdf475 // indirect
	golang.org/x/crypto v0.0.0-20220128200615-198e4374d7ed // indirect
	golang.org/x/net v0.0.0-20220127200216-cd36cc0744dd // indirect
)

replace github.com/neutrinocorp/streamhub => ../../../../

replace github.com/neutrinocorp/streamhub/streamhub-sarama => ../../

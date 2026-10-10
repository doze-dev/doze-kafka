module github.com/doze-dev/doze-kafka/module

go 1.27.0

// The module is built beside the broker it embeds, so a change to the broker and
// to its doze module is one commit. A nested module keeps the broker's own go.mod
// free of the plugin SDK and gRPC.
require (
	github.com/doze-dev/doze-kafka v1.2.0
	github.com/doze-dev/doze-sdk v0.7.1-0.20261010145324-086b5b57ecdc
	github.com/hashicorp/hcl/v2 v2.25.0
	github.com/twmb/franz-go v1.22.1
	github.com/twmb/franz-go/pkg/kadm v1.19.0
	gopkg.in/yaml.v3 v3.0.1
)

require (
	github.com/BurntSushi/toml v1.6.0 // indirect
	github.com/agext/levenshtein v1.2.1 // indirect
	github.com/apparentlymart/go-textseg/v15 v15.0.0 // indirect
	github.com/apparentlymart/go-textseg/v17 v17.0.1 // indirect
	github.com/bufbuild/protocompile v0.14.1 // indirect
	github.com/fatih/color v1.19.0 // indirect
	github.com/go-viper/mapstructure/v2 v2.4.0 // indirect
	github.com/golang/protobuf v1.5.4 // indirect
	github.com/golang/snappy v1.0.0 // indirect
	github.com/google/go-cmp v0.7.0 // indirect
	github.com/hamba/avro/v2 v2.31.0 // indirect
	github.com/hashicorp/go-hclog v1.6.3 // indirect
	github.com/hashicorp/go-plugin v1.8.0 // indirect
	github.com/hashicorp/yamux v0.1.2 // indirect
	github.com/json-iterator/go v1.1.12 // indirect
	github.com/klauspost/compress v1.20.1 // indirect
	github.com/mattn/go-colorable v0.1.15 // indirect
	github.com/mattn/go-isatty v0.0.22 // indirect
	github.com/mitchellh/go-wordwrap v1.0.1 // indirect
	github.com/modern-go/concurrent v0.0.0-20180306012644-bacd9c7ef1dd // indirect
	github.com/modern-go/reflect2 v1.0.2 // indirect
	github.com/oklog/run v1.1.0 // indirect
	github.com/pierrec/lz4/v4 v4.1.33 // indirect
	github.com/santhosh-tekuri/jsonschema/v6 v6.0.3 // indirect
	github.com/twmb/franz-go/pkg/kmsg v1.14.0 // indirect
	github.com/zclconf/go-cty v1.19.0 // indirect
	go.etcd.io/bbolt v1.5.0 // indirect
	golang.org/x/mod v0.41.0 // indirect
	golang.org/x/net v0.59.0 // indirect
	golang.org/x/sync v0.23.0 // indirect
	golang.org/x/sys v0.48.0 // indirect
	golang.org/x/text v0.42.0 // indirect
	golang.org/x/tools v0.49.0 // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20260630182238-925bb5da69e7 // indirect
	google.golang.org/grpc v1.82.0 // indirect
	google.golang.org/protobuf v1.36.12 // indirect
)

replace github.com/doze-dev/doze-kafka => ../

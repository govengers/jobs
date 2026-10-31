module github.com/govengers/jobs/examples/tracing

go 1.26.0

require (
	github.com/govengers/jobs v0.0.0-00010101000000-000000000000
	github.com/stretchr/testify v1.12.1
	go.opentelemetry.io/otel v1.47.0
	go.opentelemetry.io/otel/trace v1.47.0
)

require (
	github.com/cespare/xxhash/v2 v2.3.0 // indirect
	github.com/google/uuid v1.6.0 // indirect
	go.yaml.in/yaml/v3 v3.0.5 // indirect
	golang.org/x/sync v0.24.0 // indirect
)

// The example tracks the module it sits in; a service requires a released version instead.
replace github.com/govengers/jobs => ../..

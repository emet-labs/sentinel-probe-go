module github.com/emet-labs/sentinel/sdk/go

// No `toolchain` directive on purpose. The Justfile runs every Go recipe under
// GOTOOLCHAIN=local so an audited compiler cannot be silently replaced by a dependency;
// a `toolchain` line above the installed compiler is inert at best under that setting, and
// for a consumer without it triggers exactly the silent download the pin exists to prevent.
// devbox pins go@1.25.12, the minimum satisfying otel v1.45.0 and connect v1.20.0.
go 1.25.0

require (
	connectrpc.com/connect v1.20.0
	github.com/google/uuid v1.6.0
	go.opentelemetry.io/otel v1.45.0
	go.opentelemetry.io/otel/sdk v1.45.0
	go.opentelemetry.io/otel/trace v1.45.0
	go.opentelemetry.io/proto/otlp v1.10.0
	google.golang.org/protobuf v1.36.11
	pgregory.net/rapid v1.3.0
)

require (
	github.com/cespare/xxhash/v2 v2.3.0 // indirect
	github.com/go-logr/logr v1.4.4 // indirect
	github.com/go-logr/stdr v1.2.2 // indirect
	github.com/grpc-ecosystem/grpc-gateway/v2 v2.28.0 // indirect
	go.opentelemetry.io/auto/sdk v1.2.1 // indirect
	go.opentelemetry.io/otel/metric v1.45.0 // indirect
	golang.org/x/net v0.50.0 // indirect
	golang.org/x/sys v0.47.0 // indirect
	golang.org/x/text v0.34.0 // indirect
	google.golang.org/genproto/googleapis/api v0.0.0-20260209200024-4cfbd4190f57 // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20260209200024-4cfbd4190f57 // indirect
	google.golang.org/grpc v1.79.2 // indirect
)

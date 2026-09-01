# Go Probe SDK for Sentinel

[![CI](https://github.com/emet-labs/sentinel-probe-go/actions/workflows/ci.yml/badge.svg)](https://github.com/emet-labs/sentinel-probe-go/actions/workflows/ci.yml)
[![Go Reference](https://pkg.go.dev/badge/github.com/emet-labs/sentinel-probe-go)](https://pkg.go.dev/github.com/emet-labs/sentinel-probe-go)
[![License: MPL-2.0](https://img.shields.io/badge/License-MPL--2.0-informational.svg)](LICENSE)

The Go Probe SDK for [Sentinel](https://github.com/emet-labs) — instrument a Go service
as a Sentinel **Probe**: the in-process component that reports what your service really
did, and asks Sentinel whether to proceed before an action becomes irreversible.

Sentinel verifies cross-system action sequences against rules you declare, using
recorded evidence rather than logs and dashboards. A Probe is how your code joins that
contract.

## Requirements

- Go 1.25 or later.
- A running [Sentinel](https://github.com/emet-labs) deployment to connect to.

## Installation

```sh
go get github.com/emet-labs/sentinel-probe-go
```

The generated protobuf types are committed, so the module is consumable from a bare
clone with no code generator.

## Quickstart

```go
package main

import (
	"context"
	"time"

	"github.com/emet-labs/sentinel-probe-go/client"
	"github.com/emet-labs/sentinel-probe-go/emission"
	"github.com/emet-labs/sentinel-probe-go/enforcement"
	"github.com/emet-labs/sentinel-probe-go/ids"
)

func main() {
	// Connect to Sentinel's decision endpoint and identify this Probe.
	decider := client.NewSentinelClient(client.TransportOptions{
		BaseURL: "http://sentinel.local:7070",
	})
	probe := client.New(client.Config{SourceHandle: "checkout-service"}, decider)

	// The tracer your service emits evidence with. Attach your OTLP span
	// processor to tracer.Provider — export stays yours, always.
	tracer := emission.NewProbeTracer(emission.Options{
		TracerName:  "checkout-service",
		ServiceName: "checkout-service",
	})

	ctx := context.Background()
	_, span := tracer.Tracer.Start(ctx, "charge.card")
	// ... the work the Specification watches ...
	span.End()

	// Convert the ended span into the event Sentinel reasons about. Your span
	// processor hands you the ReadOnlySpan in OnEnd.
	event := emission.SpanToEvent(emission.SpanConversion{
		Span:              endedSpan,
		SchemaVersion:     "1.0.0",
		AcknowledgedEpoch: probe.AcknowledgedEpoch(),
	})

	// Ask before the action becomes irreversible.
	outcome := enforcement.Gate(ctx, event, probe.CurrentFilter(), nil,
		enforcement.Deps{
			Decide:              probe.DecideFunc(),
			NowMonotonicNs:      func() int64 { return time.Now().UnixNano() },
			AcceptedFailModeFor: acceptedFailMode, // what your deployment contracted to
		},
		enforcement.Options{
			SourceHandle:   probe.SourceHandle(),
			RequestID:      ids.GenerateRequestID(),
			IdempotencyKey: ids.GenerateIdempotencyKey(),
		},
	)

	switch outcome.Kind {
	case enforcement.OutcomePermit, enforcement.OutcomeFailOpenPermit, enforcement.OutcomeNoFilter:
		// No filter held means the conservative default: proceed, fail-open.
		commitTheAction()
	default: // Deny, Defer, FailClosedDeny: block, or retry within the budget.
		rollback()
	}
}
```

When Sentinel publishes a new Event Filter epoch for this source, swap it in:

```go
probe.SetFilter(newFilter) // a no-op when the epoch is unchanged
```

## What the Probe does

| Package | Duty |
|---|---|
| `client` | Holds the versioned Event Filter for your source; builds decision requests (`ProbeClient`, `NewSentinelClient`, `FilterStore`) |
| `filter` | Relevance projection before shipping — drops attributes no Specification needs. Never samples (`ApplyFilter`) |
| `emission` | OTel spans become Sentinel events (`NewProbeTracer`, `SpanToEvent`) |
| `enforcement` | The blocking decision — permit / deny / defer, with per-Specification fail modes and a monotonic latency budget (`Gate`) |
| `config` | Source-tier resolution from deployment config, never hard-coded |
| `ids`, `int128` | Per-call identifiers; the proto `Int128` helpers |

## Status

Early, pre-1.0. The wire protocol is versioned per release, but the Go API surface
may still change.

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md). This repository is a published mirror; pull
requests are reviewed and re-landed upstream.

## License

[MPL-2.0](LICENSE)

## Development environment (this mirror): Devbox + just

This repository pins its own toolchain — `devbox.json` + `devbox.lock` — and every task
runs inside it, the same convention as the Sentinel source repository:

    devbox install        # once; resolves devbox.lock
    devbox shell          # then `just --list` for the recipes

`build`, `test`, `lint` and `fmt-check` reuse the canonical gate names of the source
repository, scoped to this one language.

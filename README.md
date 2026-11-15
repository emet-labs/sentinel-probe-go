# Go Probe SDK for Sentinel

[![CI](https://github.com/emet-labs/sentinel-probe-go/actions/workflows/ci.yml/badge.svg)](https://github.com/emet-labs/sentinel-probe-go/actions/workflows/ci.yml)
[![Go Reference](https://pkg.go.dev/badge/github.com/emet-labs/sentinel-probe-go)](https://pkg.go.dev/github.com/emet-labs/sentinel-probe-go)
[![License: MPL-2.0](https://img.shields.io/badge/License-MPL--2.0-informational.svg)](LICENSE)

The Go Probe SDK for [Sentinel](https://github.com/emet-labs) — instrument a Go application
as a Sentinel **Probe**: the in-process component that reports what your application really
did, as Events, and receives Sentinel's Verdicts and Event Filters in return.

Sentinel verifies cross-system action sequences against Specifications you declare, using
recorded evidence rather than logs and dashboards. A Probe is how your code joins that
contract. Missing evidence is never a pass: until Sentinel can show it has seen everything
a source emitted, a Verdict that depends on that source stays Inconclusive.

## Requirements

- Go 1.25 or later.
- A [Sentinel](https://github.com/emet-labs) deployment on the same host as the Probe.
  The receiver uses the IPv4 loopback address `127.0.0.1`. Start it before Sentinel,
  following the order below.

## Installation

```sh
go get github.com/emet-labs/sentinel-probe-go
```

The generated protobuf types are committed, so the module is consumable from a bare
clone with no code generator.

## Quickstart

The Probe wire is OTLP/HTTP protobuf, in both directions:

- each ended OTel span is exported as one Event to Sentinel's `POST /v1/traces`, with the
  `x-sentinel-source-handle` header naming your source;
- completeness signals go to `POST /v1/logs` on the same base URL, with the same
  `x-sentinel-source-handle` header;
- Sentinel posts Verdicts and published Event Filters as OTLP logs to the Probe's loopback
  log receiver.

Start the Probe and its log receiver first. Then start Sentinel with
`SENTINEL_PROBE_LOG_URL` set to the receiver's full `http://127.0.0.1:<port>/v1/logs`
URL, and publish the Specification only after the receiver is listening. Sentinel reads
this URL once at startup and does not retry a failed Filter push. If the receiver's port
changes when the Probe restarts, restart Sentinel with the new URL and publish again.
One Sentinel process has one Probe log URL, so only one Probe process receives these
Verdicts and Event Filters today.

Both ingest routes require a source handle admitted by your Sentinel deployment; replace
`checkout-service` below with that handle. A missing header returns HTTP 401; an unknown
handle returns HTTP 403, and neither stores evidence. A completeness record's
`sentinel.source_handle` must match the header; the SDK exporter sets both.

Sentinel accepts `Content-Type: application/x-protobuf`, with identity or gzip encoding.
OTLP/JSON, OTLP/gRPC and other encodings are unsupported (HTTP 415 on the HTTP route).
An invalid span can refuse the whole request during decoding, including an unknown
`sentinel.*` attribute. Admission conflicts may instead return HTTP 200 with nonzero
`partial_success.rejected_spans`; this is still a failed export for completeness.
The OTel span name becomes the Event kind.

```go
package main

import (
	"context"
	"log"

	"github.com/google/uuid"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"

	"github.com/emet-labs/sentinel-probe-go/client"
	"github.com/emet-labs/sentinel-probe-go/emission"
	"github.com/emet-labs/sentinel-probe-go/otlp"
)

const (
	sentinelURL  = "http://127.0.0.1:8080"
	sourceHandle = "checkout-service"
)

func main() {
	ctx := context.Background()

	// The loopback log receiver: point Sentinel's SENTINEL_PROBE_LOG_URL at
	// receiver.LogsURL(). Verdicts and Event Filters arrive here.
	receiver, err := otlp.Listen("127.0.0.1:4319")
	if err != nil {
		log.Fatal(err)
	}
	defer receiver.Close()

	// Holds the versioned Event Filter; the gate reads decisions from the receiver.
	probe := client.New(client.Config{SourceHandle: sourceHandle, SentinelBaseURL: sentinelURL})
	probe.SetDecisions(receiver)

	// One sequence allocator per source (see "Sequence numbers and completeness").
	sequence, err := emission.NewSequenceAllocator(emission.SequenceOptions{})
	if err != nil {
		log.Fatal(err)
	}

	// Exports each ended OTel span as one Event to POST /v1/traces, and stamps
	// sentinel.sequence.epoch and sentinel.sequence.number when each span starts.
	tracer := emission.NewProbeTracer(emission.Options{
		TracerName:   sourceHandle,
		ServiceName:  sourceHandle,
		Endpoint:     sentinelURL,
		SourceHandle: sourceHandle,
		Sequence:     sequence,
	})

	// Declares this source's completeness on POST /v1/logs.
	heartbeat, err := otlp.NewCompletenessHeartbeat(otlp.HeartbeatConfig{
		Source: sequence,
		FilterEpoch: probe.AcknowledgedEpoch,
		Endpoint: sentinelURL,
		SourceHandle: sourceHandle,
		OnError: func(err error) { log.Print(err) },
	})
	if err != nil {
		log.Fatal(err)
	}
	go func() { _ = heartbeat.Run(ctx) }()

	// Repeat whenever a new Filter arrives, before emitting or reporting completeness.
	if filters := receiver.Filters(); len(filters) > 0 {
		probe.SetFilter(filters[len(filters)-1])
	}
	var filterEpoch int64 // 0 until the first Event Filter arrives
	if epoch := probe.AcknowledgedEpoch(); epoch != nil {
		filterEpoch = int64(*epoch)
	}
	_, span := tracer.Tracer.Start(ctx, "checkout.charge.completed", trace.WithAttributes(
		attribute.String(emission.AttributeEventID, uuid.NewString()), // your unique Event id
		attribute.String(emission.AttributeSchemaVersion, "1.0.0"),
		attribute.Int64(emission.AttributeAcknowledgedFilterEpoch, filterEpoch),
		attribute.String(emission.AttributeClaimedSensitivity, "SENSITIVITY_INTERNAL"),
		attribute.String("order.id", "order-1001"),
	))
	// ... the work the Specification watches ...
	span.End()

	// On a clean exit: flush the exports, then send the final completeness report.
	_ = tracer.Provider.Shutdown(ctx)
	heartbeat.Shutdown(ctx)
}
```

Every exported OTel span carries `sentinel.event.id`, `sentinel.schema.version`,
`sentinel.acknowledged_filter_epoch` (an `int64`, 0 until the first Event Filter arrives),
`sentinel.claimed_sensitivity` (a `Sensitivity` name other than `SENSITIVITY_UNSPECIFIED`)
and the two sequence attributes. A causal predecessor goes in `sentinel.parent.event.id`,
or in a link's own `sentinel.event.id`. An OTel span that breaks this contract is refused
before anything is sent, with an `*otlp.Refusal` reported through OpenTelemetry's error
handler; a span id never stands in for a missing Event id.

Event Filters collect on the receiver (`receiver.Filters()`, oldest first) and are not
applied for you. Hand the newest to `probe.SetFilter`, which is a no-op when the epoch is
unchanged or older. The tracer's exporter does not apply the Event Filter: it exports every ended
OTel span in full. `filter.ApplyFilter` projects a `ProducerEvent` against the held
Filter.

Start Probe spans as roots, or stamp the parent's Event id in
`sentinel.parent.event.id`: an ambient OTel parent without that Event id is refused.

### Enforcement gate

The gate is in the SDK, but Sentinel does not answer decision requests yet (see Status).

```go
import (
	"github.com/emet-labs/sentinel-probe-go/enforcement"
	"github.com/emet-labs/sentinel-probe-go/ids"
)

// event is the ProducerEvent for the action, for example emission.SpanToEvent on the
// ended span a span processor receives. Ask before the action becomes irreversible.
outcome := enforcement.Gate(ctx, event, probe.CurrentFilter(), nil,
	enforcement.Deps{
		Decide:              probe.DecideFunc(),
		NowMonotonicNs:      monotonicNanos,   // your monotonic clock, in nanoseconds
		AcceptedFailModeFor: acceptedFailMode, // the fail mode your deployment contracted
	},
	enforcement.Options{
		SourceHandle:   probe.SourceHandle(),
		RequestID:      ids.GenerateRequestID(),
		IdempotencyKey: ids.GenerateIdempotencyKey(),
	},
)
if outcome.Kind.Permitted() {
	commitTheAction() // permit, fail-open-permit or no-filter
} else {
	rollback() // deny, defer or fail-closed-deny
}
```

## What the Probe does

| Package | Duty |
|---|---|
| `otlp` | The Probe wire. `NewTraceExporter` posts ended OTel spans to `POST /v1/traces` and refuses a span that breaks the Event contract; `Listen` serves the loopback log receiver for Verdicts, Event Filters and DecideResponses; `ExportCompleteness` and `NewCompletenessHeartbeat` send completeness signals on `POST /v1/logs` |
| `emission` | OTel spans become Events (`NewProbeTracer`, `SpanToEvent`). `NewSequenceAllocator` numbers the Events |
| `client` | Holds the versioned Event Filter for your source and reads decisions from the log receiver (`ProbeClient`, `FilterStore`) |
| `filter` | Relevance projection — drops attributes no Specification needs. Never samples (`ApplyFilter`) |
| `enforcement` | The blocking decision — permit / deny / defer, with per-Specification fail modes and a monotonic latency budget (`Gate`). Not answered yet: see Status |
| `config` | Source Tier resolution from deployment config, never hard-coded (`LoadSourceTierConfig`, `TierForHandle`) |
| `ids`, `int128` | Per-call identifiers; the proto `Int128` helpers |

## Sequence numbers and completeness

To show it has seen everything a source emitted, Sentinel needs two things from the
source:

- **Sequence numbers.** Every exported Event carries `sentinel.sequence.epoch`, its
  Sequence Epoch, and `sentinel.sequence.number`: 1, 2, 3, … within that epoch.
- **Completeness signals.** A `ProducerCompletenessSignal` on `POST /v1/logs` declares, for
  the current epoch, `last_emitted` (the highest number allocated) and `complete_through`
  (the highest number up to which every Event was exported successfully), with the Filter
  epoch the Probe acknowledges. The source's Watermark advances from these signals.

The SDK provides both:

- `emission.NewSequenceAllocator(emission.SequenceOptions)` returns a `*SequenceAllocator`.
  Create one per source and set it as `emission.Options.Sequence`. The tracer then
  allocates a slot when each OTel span starts and stamps both sequence attributes. An
  OTel span that already carries both keeps them, so you can reserve a slot with
  `Allocate()` ahead of its Event and stamp it on the span yourself.
- After each export the tracer marks the slot. An export succeeded only when Sentinel
  answered HTTP 2xx with no rejected spans (`partial_success.rejected_spans` absent or 0;
  `(*otlp.TraceExporter).ExportSpansChecked` returns that count); anything else marks the
  slot failed. If you allocate a slot and then drop its Event for the Event Filter, mark
  it with `MarkFiltered(slot, filterEpoch)`.
- `otlp.NewCompletenessHeartbeat(otlp.HeartbeatConfig)` returns a
  `*CompletenessHeartbeat`. `Run(ctx)` reports the allocator's declaration every 15
  seconds by default until `ctx` is cancelled; `Report(ctx)` sends one on demand. It
  acknowledges the held Filter epoch (0 before the first Event Filter), sends one signal
  at a time, and restates the previous signal when nothing has changed. `Now`, `Ticks`
  and `Send` in the config replace its clock, its ticker and its transport.

**`complete_through` never runs ahead of the evidence.** It advances only across a
contiguous run of successful exports. A failed export, an OTel span that never ends, and
today an Event dropped for the Event Filter each hold it below that slot: the epoch stays
open and the Verdicts that depend on this source stay Inconclusive instead of passing.
The allocator does not retry. Re-export the identical Event and call
`MarkExported(slot)`.

**The epoch rolls after a declared close.** A signal whose `complete_through` equals
`last_emitted` is a declared close, and Sentinel accepts nothing more in that epoch. The
allocator seals it, and the next allocation opens a new epoch: the greater of the previous
epoch plus one and the clock's Unix milliseconds. Expect a new Sequence Epoch after every
quiet period. If Sentinel has not yet accepted the previous epoch's final declaration
under the Filter epoch the Probe now acknowledges, the heartbeat restates it before the
first signal of the new epoch.

**A restarted process needs a strictly greater epoch.** By default the first epoch is the
clock's Unix milliseconds. If your clock can step backwards across a restart, persist the
last epoch and pass it to the next run's `emission.SequenceOptions`: the allocator then
starts at the greater of the clock and that epoch plus one, and refuses an explicit epoch
that is not greater. Without it, a clock that stepped backwards yields an epoch that
Sentinel refuses with HTTP 409.

**Idle sources.** `DeclareIdle()` reports the source as intentionally idle. The next
allocation, or `DeclareActive()`, makes it active again.

**`Stop()` and `Shutdown`.** `Stop()` cancels the schedule and silences reports without
sending a final signal. Cancelling `Run`'s context only ends the schedule; you may still
call `Report(ctx)` or `Shutdown(ctx)` with a live context afterwards. `Shutdown(ctx)`
waits for a running report and sends a final declaration, which closes only if every slot
was exported. On a clean exit, shut the tracer's provider down first, then call
`Shutdown` before `Stop`.

## Status

- **Detect-only.** Sentinel ingests Events, evaluates Specifications and reports Verdicts.
  It blocks nothing.
- **Decide is not served.** Sentinel answers no decision requests today. When an enforcing
  Specification selects an Event, `ProbeClient`'s `DecideFunc` waits on the receiver for a
  DecideResponse for at most the request's remaining Transport Budget. No answer arrives,
  so when the budget runs out the gate resolves by its fail mode: `OutcomeFailClosedDeny`
  when one of those Specifications is fail-closed and your deployment accepted that,
  `OutcomeFailOpenPermit` otherwise.
- **One wire.** OTLP/HTTP protobuf, as described above, is the only Probe wire.
- **Unversioned.** There is no release yet, and no compatibility guarantee before one: the
  API and the wire may still change. A `0.1.0` package manifest is not a release.

A Specification with no blocking budget or a zero budget resolves by fail mode at once.

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

The toolchain is tested with Devbox 0.19.0. Devbox release installs are a launcher that
runs `DEVBOX_USE_VERSION` when it is set, so `export DEVBOX_USE_VERSION=0.19.0` keeps you
on the tested version.

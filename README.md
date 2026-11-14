# Go Probe SDK

The Go Probe SDK (issue #33), ported from the TypeScript reference SDK in `sdk/typescript/`
(issue #28). It implements against the locked proto contract (#27, closed). The SDK:

- holds the versioned Event Filter for its source (`client/`)
- exports ended spans as OTLP/HTTP protobuf to `/v1/traces` (`emission/`, `otlp/`)
- applies attribute-level Filter projection before shipping — relevance, never sampling
  (`filter/`, ADR-0006)
- enforces `ASK_AND_BLOCK` specs from a `DecideResponse` delivered on `POST /v1/logs`
  (`enforcement/`, the ADR-0023 bounded fragment). The live path does not dial Decide
- reads source-tier from deployment config (`config/`, ADR-0022, never hard-coded)

ADR-0002 left export to the host. #398 excepts the Probe wire: this SDK posts traces
and completeness logs, and receives Verdict, Event Filter, and DecideResponse logs.
`SentinelDecisionService` is not the wire. The same-machine test runs from
`just harness-probe-sdk-go` against the devbox-built `sentinel-periphery`.

## Generated code is not committed

`sdk/go/gen/` is **gitignored** and produced at build time by `tools/generate-go-sdk.sh`
(driven by `sdk/go/buf.gen.go.yaml`), mirroring how `sdk/typescript` produces
`.generated/typescript/`. Consequences:

- **This module is not `go get`-consumable from a bare clone.** Run `just build` (or
  `tools/generate-go-sdk.sh`) first, or `gopls` and `go build` will fail on missing packages.
- `rm -rf sdk/go/gen` forces a full regeneration; so does editing any `.proto` or the
  codegen template, which `sdk/go/gen/.proto-digest` detects.

The repository's precedent for committed generated output (`gen/rust/`) is
committed-**and**-drift-gated: `tools/check-generated-probes.sh` regenerates the whole tree
twice and compares both against each other and against the committed copy. Committing Go
without an equivalent gate would give the worst of both worlds, and building that gate is a
materially larger change than this SDK.

## Package layout

Mirrors `sdk/typescript/src/` one-for-one where Go idioms allow.

| Go package | TypeScript source |
| --- | --- |
| `client/` | `src/client/` (`filter-store.ts`, `probe-client.ts`, `transport.ts`) |
| `filter/` | `src/filter/apply-filter.ts` |
| `internal/specmatch/` | the non-exported half of `apply-filter.ts` (`specSelects`) |
| `emission/` | `src/emission/` (`span-to-event.ts`, `otel-bridge.ts`) |
| `enforcement/` | `src/enforcement/` (`enforcement-gate.ts`, `monotonic-budget.ts`) |
| `config/` | `src/config/` (`source-tier.ts` + `schema.ts`, collapsed) |
| `int128/` | `src/util/int128.ts` |
| `ids/` | `src/util/id.ts` |
| `propagate/` | (Go-first) parent Event id header and tracestate carrier |

There is deliberately **no root `.go` file**: the module directory is literally named `go`,
and Go has no re-export idiom that would make a barrel package worth the ceremony. This
README and the per-package `doc.go` files carry what `src/index.ts`'s docblock carried.

Import aliases used throughout:

```go
modelv1 "github.com/emet-labs/sentinel/sdk/go/gen/sentinel/model/v1"
probev1 "github.com/emet-labs/sentinel/sdk/go/gen/sentinel/probe/v1"
"github.com/emet-labs/sentinel/sdk/go/gen/sentinel/probe/v1/probev1connect"
```

`filter.proto` is in proto package `sentinel.model.v1`, so `EventFilter`,
`SpecificationFilter`, `FailMode` and `DeliveryMode` all live under `modelv1`.

## Sequence numbers and completeness

`emission.SequenceAllocator` and `otlp.CompletenessHeartbeat` (#461) give a source its
`(Sequence Epoch, sequence number)` slots and its producer completeness signal, so a host
does not hand-roll either.

```go
allocator, err := emission.NewSequenceAllocator(emission.SequenceOptions{})
tracer := emission.NewProbeTracer(emission.Options{
	TracerName: "sagashop-order", Endpoint: base, SourceHandle: "sagashop-order",
	Sequence: allocator,
})
heartbeat, err := otlp.NewCompletenessHeartbeat(otlp.HeartbeatConfig{
	Source: allocator, FilterEpoch: filterStore.Epoch,
	Endpoint: base, SourceHandle: "sagashop-order",
})
go heartbeat.Run(ctx) // every 15 s by default
```

- **Slots.** With `Options.Sequence`, each span gets `sentinel.sequence.epoch` and
  `sentinel.sequence.number` when it starts, unless it already carries both (a slot the
  host reserved with `Allocate`). The slot is marked from Sentinel's answer: exported only
  on HTTP 2xx with zero `partial_success.rejected_spans`
  (`TraceExporter.ExportSpansChecked`, `otlp.RejectedSpans`); failed on any other answer,
  an undecodable body or a client-side refusal. Pass the sequence attributes at start or
  not at all.
- **What the signal says.** `last_emitted` is the highest number allocated.
  `complete_through` is the highest number below which every slot was exported, so it
  never runs ahead of a successful export. A failed slot holds `complete_through`; the
  default exporter retries it (see "Export is asynchronous and bounded" below), or the host
  re-exports the same Event and calls `MarkExported`.
  A slot the Event Filter dropped (`MarkFiltered`) also holds it until #542.
- **One Sequence Epoch per quiet period.** A report that finds nothing in flight declares
  a close (`complete_through == last_emitted`), and Sentinel refuses a closed epoch that
  advances. So the allocator seals the epoch, and the next `Allocate` opens
  `max(epoch + 1, clock Unix ms)`. Sentinel evaluates each source's latest epoch, so a
  Binding whose Events straddle a roll of one source is Inconclusive, never a pass.
  Only the epoch current when a Filter is published can settle Authoritative. A source
  that is quiet at publication and wants Authoritative Verdicts should `Allocate` first and
  start the Event's span later with that slot.
- **Restarts.** The first epoch is the clock's Unix milliseconds. Persist the last epoch and
  pass it as `PreviousEpoch` to guarantee a strictly greater epoch when the clock steps
  back; without it Sentinel refuses the regressed epoch (`RegressedEpoch`, HTTP 409).
- **Heartbeats are restatements.** Between quiet reports only `producer_reported_at`
  changes, so Sentinel writes no Verdict revision. `producer_reported_at` never goes
  backwards and never precedes `activity_since`. `DeclareIdle` and `DeclareActive` set the
  activity; the next allocation ends an idle. A regressed acknowledged Filter epoch is
  never sent: `OnError` receives `HeartbeatFilterEpochRegressed`.
- **Stopping.** Cancelling `Run`'s context ends the schedule. `Shutdown` sends one final
  report, which is a close only if nothing is unresolved, then stops. `Stop` makes the
  source go silent and sends nothing more.

## Callers that are not Probes

A span whose parent is remote and carries no `sentinel.parent.event.id` (a browser, a curl, an
uninstrumented or Rust service called it) is refused by default: the export names no causal edge and
a span id never stands in for an Event id. A service that has such callers sets
`emission.Options.Export.AllowUnattributedRemoteParent` (or `otlp.TraceConfig.Export`): the Event is then
exported with no edge to that caller, so a Binding that needs the edge stays Inconclusive instead of the
whole source's `complete_through` stopping at the refused slot. A local parent without the id is still
refused.

## Export is asynchronous and bounded

With `Options.Endpoint` set, `NewProbeTracer` exports from a bounded queue on its own goroutine
(`emission.AsyncProcessor`), so an unreachable or slow Sentinel never adds latency to `span.End()`.
The span-end path never panics, never returns an error into the host, and never blocks: any
validation, encode or transport failure marks the slot FAILED (`complete_through` never passes
it), is passed to `Options.OnError` as an `*emission.ExportError` (with `Code`, span name, slot and
cause) and is logged at most once per `ErrorLogInterval` (default 10 s). Tune it with
`Options.Async` (`emission.AsyncOptions`):

- **Overflow.** The queue holds `QueueSize` spans (2048). A span that ends while it is full is dropped,
  not waited for, and its slot is FAILED (`queue-full`).
- **Retry.** A batch is retried up to `MaxAttempts` (5) times with exponential backoff and jitter on
  transport errors and HTTP 408/425/429/5xx only. A contract failure, a 400/403 and a Sentinel
  rejection get the same answer again and are not retried. After the last attempt the slots are FAILED
  and the spans stay in a buffer of `RetainFailed` (1024) that is retried every `RetryFailedInterval`
  (30 s) and once at shutdown, so `complete_through` catches up when Sentinel returns.
- **A bad span among good ones** is isolated by sending the batch's spans one by one.
- **Duplicate Event id.** Sentinel refuses a redelivery of an Event id it already holds under other
  content (`evidence_conflict`, `identity_conflict`). The refusal is deterministic, so it is not
  retried, and the slot stays FAILED: that slot number never reached Sentinel as an Event, so
  `complete_through` does not pass it (missing evidence never looks complete). The refusal is reported
  as `duplicate-event-id`; the Event under that id stays what Sentinel accepted first. Any other
  deterministic rejection (`slot_conflict`, a closed boundary) also leaves the slot FAILED.
- **Shutdown.** `ProbeTracer.Shutdown(ctx)` (or `Provider.Shutdown`) flushes the queue, retries retained
  spans once and stops. Call it before the heartbeat's own `Shutdown` so the final report declares what
  was exported. `Provider.ForceFlush` flushes the queue without stopping.
- `AsyncOptions.Synchronous` exports on the ending goroutine (tests, one-shot tools).
- Export errors carry Sentinel's response body: `*otlp.HTTPStatusError` has `StatusCode` and `Body`
  and unwraps to the older `*otlp.Refusal`; `otlp.Retryable(err)` says what the exporter retries.

## Stamping the reserved attributes

The host owns `sentinel.event.id`, `sentinel.parent.event.id`, `sentinel.schema.version`,
`sentinel.acknowledged_filter_epoch` and `sentinel.claimed_sensitivity`. Setting them per span still
works. `Options.HostStamp` (`emission.HostStamperOptions`, or `emission.NewHostStamper` as a processor)
stamps all five at span start so a service does not: an Event id `<handle>:<uuid>`, the parent span's
Event id (or the remote caller's, below), the schema version, the acknowledged Filter epoch
(`FilterStore.Epoch`) and the claimed sensitivity. An attribute the host already set is never overwritten.

## Carrying the parent Event id across services

`propagate.InjectParentEventID(carrier, eventID)` and `propagate.ExtractParentEventID(carrier)` write and read
the `x-sentinel-parent-event-id` header, with the `sentinel` member of `tracestate` as the extract fallback.
A `propagate.Carrier` is `Get`/`Set` (an `http.Header` fits; wrap Kafka headers in two methods).
`otel.SetTextMapPropagator(propagate.Propagator{Inner: propagation.TraceContext{}})` does it for every
instrumented client and server, and `HostStamper` then names the caller's Event as the parent. A caller
that is not a Probe sends neither, and the callee exports its Event as a root.

## Receiving Event Filters in a container

`otlp.ListenWith(otlp.ListenConfig{Host: "0.0.0.0", Port: 4318})` binds the log receiver on every
interface (the default `Host` is loopback, and `otlp.Listen` still refuses anything else) so Sentinel can
POST Event Filters to a Probe in another container. `receiver.LogsURLFor("shop-payment")` is the URL to
register. The endpoint is unauthenticated: bind it only on a network that carries Sentinel.

## Divergences from the TypeScript reference

Deliberate, so a reviewer can check them off rather than flag them as omissions. Criterion 4
of #33 is "structure and contract mirror the TypeScript reference SDK"; everything below is a
place where Go forces a difference, or where this port is knowingly ahead of the reference.

| # | Divergence | Why |
| --- | --- | --- |
| D1 | No `src/index.ts` barrel equivalent | The module directory is `go`; `package go` is illegal, and Go has no clean re-export idiom for enum constants. Overview lives here and in per-package `doc.go`. |
| D2 | Tests beside sources, not in a parallel `tests/` tree | Go convention; same-package `_test.go` can reach unexported helpers that the TS port had to `export`. |
| D3 | No `generated.ts` re-export shim | Needed in TS only because of the `@sentinel-proto-gen` path alias. Go imports packages directly under the fixed `modelv1`/`probev1` alias convention. |
| D4 | `schema.ts` + `source-tier.ts` collapsed into `config/sourcetier.go` | No `zod`; the schema is a struct plus a validation function. |
| D5 | Enum constants keep the proto prefix | `protoc-gen-go` does not strip prefixes: `modelv1.FailMode_FAIL_MODE_CLOSED`, not `FailMode.CLOSED` as `bufbuild/es` emits. |
| D6 | `FilterStore` uses `atomic.Pointer` | Go is genuinely concurrent, so the TS "reference-swap semantics" comment becomes an enforced invariant, verified under `go test -race`. |
| D8 | `math/big.Int` for Int128 | Go has no native `int128`; `big.Int` is the `bigint` analog. |
| D9 | `pgregory.net/rapid` instead of `fast-check` | `testing/quick` is frozen and cannot reflectively construct protobuf messages (unexported `protoimpl` fields). Shrinking and seed reproduction are the wins. Test-only dependency. |
| D10 | Adds a Connect loopback test over `httptest` | Cheap in Go stdlib; strictly more coverage than the reference, which declined it. |
| D14 | Integer/double attribute dispatch is type-directed, not value-directed | TS branches on `Number.isInteger(value)`, so JS `3` and `3.0` both become `integer_value`. Go dispatches on `attribute.INT64` vs `attribute.FLOAT64`, so `float64(3)` stays a `double_value`. Go's behaviour is the correct one. |
| D15 | `GateOutcome.Reason` is populated on every outcome | TS carries `reason` only on the two fail-mode arms of a discriminated union; Go has no sum types, so one struct is forced. |
| D16 | `emission.SpanConversion`, not `SpanContext` | `trace.SpanContext` appears in the same file (`ReadOnlySpan.Parent()`, `Link.SpanContext`); mirroring the TS name would collide. |
| D17 | `Gate` short-circuits an already-dead `context.Context` into the aggregate fail mode without asking | The TS gate has no context to check. Go convention makes `ctx` the first parameter, and a real Connect client over `net/http` would fail identically; checking makes the outcome independent of whether a given `Decide` honours `ctx`. Never a permit, never a panic. `ctx` is still **not** a budget source — the proto budget is monotonic-relative and conflating the two is a clock-domain error. |

Ahead of the reference — **additions, not ports**, called out so they are not read as drift:

| # | Addition | Why the reference lacks it |
| --- | --- | --- |
| D11 | `GateOutcome.Specifications` carries per-spec `SpecificationDecision` including `UnresolvedReason` | `enforcement-gate.ts`'s `GateOutcome` has no `specifications` field; `SpecificationDecision` is a bare re-export and never populated. |
| D12 | `enforcement/doc.go` states which ADR-0023 gates are promotion-time and re-checked nowhere at runtime | `grep -rn "promotion" sdk/typescript/src` returns nothing. |
| D13 | `ApplyFilter` always allocates a fresh attribute slice | `apply-filter.ts:44` aliases `event.attributes` directly in the project-all branch, with no aliasing note. |

D7 (`bytes_value`/`map_value` unreachable through the OTel Go API) is **deleted**, not merely
reworded: it was an artefact of pinning otel at v1.31.0. At the pinned v1.45.0 the
`attribute.Type` set is `EMPTY, BOOL, INT64, FLOAT64, STRING, BOOLSLICE, INT64SLICE,
FLOAT64SLICE, STRINGSLICE, BYTESLICE, SLICE, MAP`, so all seven `AttributeValue` oneof arms
are reachable natively and no `ExtraAttributes` escape hatch is needed.

## Toolchain

Pinned `go@1.25.12` in `devbox.json` — the exact minimum satisfying the `go 1.25.0` directives
of `go.opentelemetry.io/otel` v1.45.0 (where `attribute.MAP` landed) and
`connectrpc.com/connect` v1.20.0. `go.mod` declares `go 1.25.0` with **no `toolchain`
directive**: under `GOTOOLCHAIN=local` (set per-recipe in the `Justfile`) a `toolchain` line
above the installed compiler is at best inert, and for a consumer without `GOTOOLCHAIN=local`
it triggers exactly the silent toolchain download that pinning exists to prevent.

Consistency with SagaShop's Go port is deliberately **not** a rationale for any pin here:
separate modules, independent dependency graphs, no shared build.

# Go Probe SDK

The Go Probe SDK (issue #33), ported from the TypeScript reference SDK in `sdk/typescript/`
(issue #28). It implements against the locked proto contract (#27, closed). The SDK:

- connects to Sentinel and holds the versioned Event Filter for its source (`client/`)
- wraps the OTel Go SDK to produce spans the OTLP Adapter normalizes (`emission/`)
- applies attribute-level Filter projection before shipping — relevance, never sampling
  (`filter/`, ADR-0006)
- enforces `ASK_AND_BLOCK` specs via the decision endpoint (`enforcement/`, the ADR-0023
  bounded fragment)
- reads source-tier from deployment config (`config/`, ADR-0022, never hard-coded)

The live SagaShop integration test is blocked on **#22** (enforcement decision seam) and
**#23** (production OTLP Adapter): no `SentinelDecisionService` is served anywhere in this
repository, so there is nothing to integrate against. #10 closed by delivering a pure library
evaluator in `core/`, not a server, and is not the blocker. The SDK itself and its unit tests
against the proto contract are buildable now.

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

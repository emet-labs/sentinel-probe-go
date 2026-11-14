// Package client holds the Probe's versioned Event Filter and builds decision
// requests. It is the Go analog of sdk/typescript/src/client/.
//
// The live path does not dial Decide. Verdicts, Event Filters, and
// DecideResponses arrive as OTLP logs. SetDecisions installs the receiver the
// gate reads. NewSentinelClient remains compiled and is not that path.
//
// # Filter delivery
//
// ADR-0006 says Sentinel pushes Event Filters. The push is an OTLP log whose
// payload type is sentinel.model.v1.EventFilter. This package holds and
// refreshes a filter; the host calls SetFilter when the receiver delivers one,
// and RefreshOnEpoch no-ops unless the epoch is strictly newer. SetFilter refuses
// an older or epoch-less Filter, so a reordered push never downgrades the store.
//
// # Two presence traps that the TypeScript reference could not have
//
// EventFilter.epoch is `optional uint64`, so protoc-gen-go emits an *uint64 field plus a
// nil-safe GetEpoch() that returns 0 for nil. Epoch 0 is a legitimate epoch, so:
//
//   - presence is `f.Epoch == nil`, NEVER `f.GetEpoch() == 0`; and
//   - ordering is a dereferenced comparison (see replaces), NEVER `a.Epoch == b.Epoch`,
//     which compares *pointers* and is false for distinct allocations holding the same value.
//
// The TypeScript port got both free from `bigint | undefined` and a `===` that compares
// bigints by value. A naive transliteration of filter-store.ts would make Set compare
// pointers. Both traps have dedicated table rows in filterstore_test.go.
package client

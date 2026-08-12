// Package client holds the Probe's connection to Sentinel and the versioned Event Filter for
// its source. It is the Go analog of sdk/typescript/src/client/.
//
// # Filter delivery is out of SDK scope
//
// proto/sentinel/probe/v1/decision.proto declares exactly one RPC, the unary Decide. ADR-0006
// says Sentinel pushes Event Filters to Probes, but no wire form for that push exists yet. So
// this package holds and refreshes a filter; the host delivers it by calling SetFilter when a
// push arrives, and RefreshOnEpoch is the guard that no-ops on an unchanged epoch. Identical
// to the TypeScript reference.
//
// # Two presence traps that the TypeScript reference could not have
//
// EventFilter.epoch is `optional uint64`, so protoc-gen-go emits an *uint64 field plus a
// nil-safe GetEpoch() that returns 0 for nil. Epoch 0 is a legitimate epoch, so:
//
//   - presence is `f.Epoch == nil`, NEVER `f.GetEpoch() == 0`; and
//   - equality is a dereferenced comparison (see equalEpoch), NEVER `a.Epoch == b.Epoch`,
//     which compares *pointers* and is false for distinct allocations holding the same value.
//
// The TypeScript port got both free from `bigint | undefined` and a `===` that compares
// bigints by value. A naive transliteration of filter-store.ts:33-36 would make Set report
// "updated" on every push. Both traps have dedicated table rows in filterstore_test.go.
package client

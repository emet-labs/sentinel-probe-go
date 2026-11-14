package client

import (
	"sync/atomic"

	modelv1 "github.com/emet-labs/sentinel/sdk/go/gen/sentinel/model/v1"
)

// FilterStore holds the current EventFilter for the Probe's source and tracks the
// acknowledged epoch. It is the Go analog of filter-store.ts.
//
// Reference-swap semantics: Set replaces the held pointer atomically and Get returns that
// stable pointer. The store never mutates a held EventFilter in place, so a caller that
// snapshots the filter once for an emit-and-enforce flow holds a consistent view even if a
// Set lands mid-flow.
//
// In JavaScript that property was free — the runtime is single-threaded and the TypeScript
// reference documents it as a comment. In Go it is a real concurrency requirement: a Probe
// emits from many goroutines while a filter push arrives on another. atomic.Pointer makes it
// an enforced invariant rather than a convention, verified under `go test -race`.
//
// The zero value is a usable, empty store. All methods are safe for concurrent use.
type FilterStore struct {
	current atomic.Pointer[modelv1.EventFilter]
}

// NewFilterStore returns a store optionally seeded with an initial filter (for example one
// restored from a local cache before the first push). A nil initial filter means no filter
// is held.
func NewFilterStore(initial *modelv1.EventFilter) *FilterStore {
	store := &FilterStore{}
	if initial != nil {
		store.current.Store(initial)
	}
	return store
}

// Get returns the held EventFilter, or nil before the first Set. The returned pointer is
// stable until the next Set: it is safe to hold it across an entire emit-and-enforce flow.
// Callers must not mutate the pointed-to filter.
func (s *FilterStore) Get() *modelv1.EventFilter {
	return s.current.Load()
}

// Epoch returns the held filter's epoch, or nil when no filter is held or the held filter
// declares no epoch. The result is a fresh allocation, so a caller cannot reach into the
// held filter through it.
//
// nil means "absent". It does not mean zero: epoch 0 is a legitimate epoch and is returned
// as a non-nil pointer to 0.
func (s *FilterStore) Epoch() *uint64 {
	filter := s.current.Load()
	if filter == nil || filter.Epoch == nil {
		return nil
	}
	epoch := *filter.Epoch
	return &epoch
}

// Set swaps in a new filter under the epoch order, reporting whether the store was actually
// updated. Epochs are unsigned and only move forward (ADR-0006). With held the held filter's
// epoch and new the offered one's, either of which may be absent:
//
//   - Nothing held: any filter applies, including epoch 0 and an epoch-less one.
//   - Held is absent: a filter with an epoch (including 0) replaces it; an epoch-less one is
//     a no-op.
//   - Held is present: only a strictly greater epoch replaces it. An equal epoch is a no-op,
//     the ordinary at-least-once re-delivery. A lower or absent epoch is refused, so a
//     reordered or replayed push never downgrades the filter or the acknowledged epoch.
//
// A Set that returns false keeps the held pointer and changes no later comparison; it raises
// no error and logs nothing. A nil filter counts as epoch-less.
//
// An equal epoch is never applied, even when its content differs. The server rewrites the
// current epoch in place when its Readiness flips Warming to Active, so whether a Probe
// should accept that flip at an equal epoch is an open question (#534); the store
// keeps its earlier behaviour until it is decided. After a server store reset, epochs
// restart from 1 and the store refuses them until the Probe restarts; restarting is the
// recovery.
//
// Mirrors filter-store.ts. The compare-and-swap loop keeps the order under concurrent
// pushes: two racing Sets can never leave the lower epoch held.
func (s *FilterStore) Set(filter *modelv1.EventFilter) bool {
	offered := epochOf(filter)
	for {
		held := s.current.Load()
		if held != nil && !replaces(held.Epoch, offered) {
			return false // equal, older, or epoch-less: keep the held filter
		}
		if s.current.CompareAndSwap(held, filter) {
			return true
		}
	}
}

// ShouldRefresh reports whether an announced epoch is strictly newer than the held one,
// mirroring filter-store.ts. No epoch held means refresh; no announced epoch means there is
// nothing to compare against, so do not refresh.
func (s *FilterStore) ShouldRefresh(newEpoch *uint64) bool {
	held := s.Epoch()
	if held == nil {
		return true // no epoch held -> refresh
	}
	if newEpoch == nil {
		return false // no new epoch to compare
	}
	return *newEpoch > *held
}

// replaces reports whether a filter at offered replaces one held at held, comparing the
// optional epochs BY VALUE.
//
// The dereference matters: a direct transliteration of the TypeScript comparison would
// compare *uint64 POINTERS in Go, and two distinct allocations holding the same epoch would
// look different. Neither `go vet` nor `staticcheck` catches that, which is why
// filterstore_test.go asserts it with two distinct allocations holding the same value.
func replaces(held, offered *uint64) bool {
	if offered == nil {
		return false
	}
	if held == nil {
		return true
	}
	return *offered > *held
}

// epochOf is nil-safe on the message itself, unlike the generated GetEpoch, which flattens
// an absent epoch to 0.
func epochOf(filter *modelv1.EventFilter) *uint64 {
	if filter == nil {
		return nil
	}
	return filter.Epoch
}

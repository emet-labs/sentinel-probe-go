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

// Set swaps in a new filter. It reports whether the store was actually updated, which is
// true when the epoch changed or when this is the first Set, and false when an equal epoch
// is re-pushed.
//
// Mirrors filter-store.ts:31-38, including the subtle case where the held filter and the new
// filter both carry no epoch: that still counts as an update on the first Set, because the
// "not first set" conjunct is what makes the no-op branch reachable.
func (s *FilterStore) Set(filter *modelv1.EventFilter) bool {
	held := s.current.Load()
	if held != nil && equalEpoch(held.Epoch, epochOf(filter)) {
		return false // epoch unchanged and not the first set -> no update
	}
	s.current.Store(filter)
	return true
}

// ShouldRefresh reports whether an announced epoch differs from the held one, mirroring
// filter-store.ts:39-44. No filter held means refresh; no announced epoch means there is
// nothing to compare against, so do not refresh.
func (s *FilterStore) ShouldRefresh(newEpoch *uint64) bool {
	held := s.Epoch()
	if held == nil {
		return true // no filter held -> refresh
	}
	if newEpoch == nil {
		return false // no new epoch to compare
	}
	return !equalEpoch(held, newEpoch)
}

// equalEpoch compares two optional epochs BY VALUE.
//
// This helper exists because the direct transliteration of TypeScript's
// `oldEpoch === filter.epoch` is `held.Epoch == filter.Epoch` in Go, which compares *uint64
// POINTERS. Two distinct allocations holding the same epoch compare unequal, so Set would
// report "updated" on every push and the unchanged-epoch no-op would never fire. The
// TypeScript `===` compares `bigint | undefined` by value; Go needs the dereference.
//
// Neither `go vet` nor `staticcheck` catches this, which is why filterstore_test.go asserts
// it with two distinct allocations holding the same value.
func equalEpoch(a, b *uint64) bool {
	if a == nil || b == nil {
		return a == nil && b == nil
	}
	return *a == *b
}

// epochOf is nil-safe on the message itself, unlike the generated GetEpoch, which flattens
// an absent epoch to 0.
func epochOf(filter *modelv1.EventFilter) *uint64 {
	if filter == nil {
		return nil
	}
	return filter.Epoch
}

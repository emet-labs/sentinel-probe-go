package client

import (
	"sync"
	"testing"

	modelv1 "github.com/emet-labs/sentinel/sdk/go/gen/sentinel/model/v1"
)

func u64(v uint64) *uint64 { return &v }

func makeFilter(epoch *uint64) *modelv1.EventFilter {
	return &modelv1.EventFilter{Epoch: epoch, Specifications: nil}
}

func TestFilterStoreGetIsNilBeforeFirstSet(t *testing.T) {
	t.Parallel()

	store := NewFilterStore(nil)
	if store.Get() != nil {
		t.Fatal("Get must be nil before the first Set")
	}
	if store.Epoch() != nil {
		t.Fatal("Epoch must be nil before the first Set")
	}
}

func TestFilterStoreZeroValueIsUsable(t *testing.T) {
	t.Parallel()

	var store FilterStore
	if store.Get() != nil || store.Epoch() != nil {
		t.Fatal("the zero value must behave as an empty store")
	}
	if !store.Set(makeFilter(u64(1))) {
		t.Fatal("first Set on a zero-value store must report an update")
	}
}

func TestFilterStoreSetFirstSwapsPointer(t *testing.T) {
	t.Parallel()

	store := NewFilterStore(nil)
	filter := makeFilter(u64(5))
	if !store.Set(filter) {
		t.Fatal("first Set must report an update")
	}
	if store.Get() != filter {
		t.Fatal("Get must return the exact pointer that was Set")
	}
	if got := store.Epoch(); got == nil || *got != 5 {
		t.Fatalf("Epoch = %v, want 5", got)
	}
}

func TestFilterStoreSetUnchangedEpochIsNoOp(t *testing.T) {
	t.Parallel()

	store := NewFilterStore(nil)
	first := makeFilter(u64(5))
	store.Set(first)
	if store.Set(makeFilter(u64(5))) {
		t.Fatal("re-pushing an equal epoch must not report an update")
	}
	if store.Get() != first {
		t.Fatal("the original pointer must still be held after a no-op Set")
	}
}

// TestFilterStoreSetComparesEpochsByValueNotPointer is the regression test for the trap that
// a naive transliteration of filter-store.ts:33-36 walks straight into. TypeScript's
// `oldEpoch === filter.epoch` compares `bigint | undefined` BY VALUE; the direct Go form
// `held.Epoch == filter.Epoch` compares *uint64 POINTERS, which are distinct allocations on
// essentially every real push. Without the dereference, Set reports "updated" every time and
// the no-op test above passes only by coincidence (the compiler may or may not share the
// literal). Here the two allocations are unmistakably distinct.
func TestFilterStoreSetComparesEpochsByValueNotPointer(t *testing.T) {
	t.Parallel()

	firstEpoch := uint64(5)
	secondEpoch := uint64(5)
	if &firstEpoch == &secondEpoch {
		t.Fatal("test precondition: the two epochs must be distinct allocations")
	}

	store := NewFilterStore(nil)
	store.Set(&modelv1.EventFilter{Epoch: &firstEpoch})
	if store.Set(&modelv1.EventFilter{Epoch: &secondEpoch}) {
		t.Fatal("distinct *uint64 allocations holding the same epoch must compare equal")
	}
}

func TestFilterStoreSetChangedEpochSwaps(t *testing.T) {
	t.Parallel()

	store := NewFilterStore(nil)
	store.Set(makeFilter(u64(5)))
	next := makeFilter(u64(6))
	if !store.Set(next) {
		t.Fatal("a changed epoch must report an update")
	}
	if store.Get() != next {
		t.Fatal("Get must return the newly set pointer")
	}
}

// TestFilterStoreEpochZeroIsPresent pins the epoch-0 trap: epoch 0 is a legitimate epoch, and
// presence must be tested with `Epoch == nil` rather than `GetEpoch() == 0`.
func TestFilterStoreEpochZeroIsPresent(t *testing.T) {
	t.Parallel()

	store := NewFilterStore(makeFilter(u64(0)))
	epoch := store.Epoch()
	if epoch == nil {
		t.Fatal("epoch 0 must be reported as present")
	}
	if *epoch != 0 {
		t.Fatalf("Epoch = %d, want 0", *epoch)
	}
	if store.ShouldRefresh(u64(0)) {
		t.Fatal("announced epoch 0 against held epoch 0 must not refresh")
	}
	if !store.ShouldRefresh(u64(1)) {
		t.Fatal("announced epoch 1 against held epoch 0 must refresh")
	}
	if store.Set(makeFilter(u64(0))) {
		t.Fatal("re-pushing epoch 0 must be a no-op, not an update")
	}
}

// TestFilterStoreSetOnFilterWithoutEpoch covers the awkward corner of filter-store.ts:33: a
// held filter and a new filter that both declare no epoch. The first Set updates because the
// `current !== undefined` conjunct is false; the second does not.
func TestFilterStoreSetOnFilterWithoutEpoch(t *testing.T) {
	t.Parallel()

	store := NewFilterStore(nil)
	if !store.Set(makeFilter(nil)) {
		t.Fatal("first Set must update even when the filter declares no epoch")
	}
	if store.Epoch() != nil {
		t.Fatal("a filter without an epoch must report a nil epoch")
	}
	if store.Set(makeFilter(nil)) {
		t.Fatal("a second epochless Set must be a no-op")
	}
	if !store.Set(makeFilter(u64(0))) {
		t.Fatal("moving from absent epoch to epoch 0 must be an update")
	}
}

func TestFilterStoreShouldRefreshWithNoFilterHeld(t *testing.T) {
	t.Parallel()

	store := NewFilterStore(nil)
	if !store.ShouldRefresh(nil) {
		t.Fatal("no filter held must refresh")
	}
	if !store.ShouldRefresh(u64(1)) {
		t.Fatal("no filter held must refresh even with an announced epoch")
	}
}

func TestFilterStoreShouldRefreshWithNoAnnouncedEpoch(t *testing.T) {
	t.Parallel()

	store := NewFilterStore(makeFilter(u64(5)))
	if store.ShouldRefresh(nil) {
		t.Fatal("nothing to compare against must not refresh")
	}
}

func TestFilterStoreShouldRefreshComparesEpochs(t *testing.T) {
	t.Parallel()

	store := NewFilterStore(makeFilter(u64(5)))
	if !store.ShouldRefresh(u64(6)) {
		t.Fatal("a differing epoch must refresh")
	}
	if store.ShouldRefresh(u64(5)) {
		t.Fatal("an equal epoch must not refresh")
	}
}

func TestFilterStoreEpochIsACopy(t *testing.T) {
	t.Parallel()

	filter := makeFilter(u64(5))
	store := NewFilterStore(filter)
	epoch := store.Epoch()
	*epoch = 99
	if got := store.Epoch(); got == nil || *got != 5 {
		t.Fatalf("mutating the returned epoch must not reach the held filter: got %v", got)
	}
	if *filter.Epoch != 5 {
		t.Fatalf("held filter epoch mutated to %d", *filter.Epoch)
	}
}

// TestFilterStoreSnapshotSurvivesMidFlowSet is the reference-swap property that the
// TypeScript version documents as a comment. A caller that snapshots the filter for an
// emit-and-enforce flow keeps a consistent view even when a push lands mid-flow.
func TestFilterStoreSnapshotSurvivesMidFlowSet(t *testing.T) {
	t.Parallel()

	store := NewFilterStore(nil)
	first := makeFilter(u64(5))
	store.Set(first)

	snapshot := store.Get()
	store.Set(makeFilter(u64(6)))

	if snapshot != first {
		t.Fatal("the snapshot must still point at the filter held when it was taken")
	}
	if store.Get() == first {
		t.Fatal("the store must have swapped to the new filter")
	}
	if got := snapshot.GetEpoch(); got != 5 {
		t.Fatalf("the snapshotted filter was mutated in place: epoch = %d", got)
	}
}

// TestFilterStoreConcurrentGetSet is the reason FilterStore uses atomic.Pointer at all. Run
// under `go test -race`, which is how the Justfile invokes it.
func TestFilterStoreConcurrentGetSet(t *testing.T) {
	t.Parallel()

	store := NewFilterStore(makeFilter(u64(0)))

	const writers, readers, iterations = 4, 8, 200
	var wg sync.WaitGroup

	for w := range writers {
		wg.Add(1)
		go func(base uint64) {
			defer wg.Done()
			for i := range uint64(iterations) {
				store.Set(makeFilter(u64(base*iterations + i + 1)))
			}
		}(uint64(w))
	}
	for range readers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range iterations {
				snapshot := store.Get()
				if snapshot == nil {
					t.Error("Get returned nil after the store was seeded")
					return
				}
				// A snapshot must be internally consistent for as long as it is held.
				first := snapshot.GetEpoch()
				if snapshot.GetEpoch() != first {
					t.Error("held filter changed under the reader — it was mutated in place")
					return
				}
				_ = store.Epoch()
			}
		}()
	}
	wg.Wait()
}

func TestEqualEpoch(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name string
		a, b *uint64
		want bool
	}{
		{"both nil", nil, nil, true},
		{"left nil", nil, u64(0), false},
		{"right nil", u64(0), nil, false},
		{"equal zero", u64(0), u64(0), true},
		{"equal non-zero", u64(7), u64(7), true},
		{"different", u64(7), u64(8), false},
		{"max", u64(^uint64(0)), u64(^uint64(0)), true},
	}
	for _, tc := range cases {
		if got := equalEpoch(tc.a, tc.b); got != tc.want {
			t.Errorf("%s: equalEpoch = %v, want %v", tc.name, got, tc.want)
		}
	}
}

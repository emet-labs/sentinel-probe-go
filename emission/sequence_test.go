package emission_test

import (
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/emet-labs/sentinel/sdk/go/emission"
	"github.com/emet-labs/sentinel/sdk/go/otlp"
)

const epochE uint64 = 1_794_000_000_000

// manualClock is an injected clock; no test reads the wall clock.
type manualClock struct {
	mu sync.Mutex
	ms int64
}

func (c *manualClock) now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return time.UnixMilli(c.ms)
}

func (c *manualClock) set(ms int64) {
	c.mu.Lock()
	c.ms = ms
	c.mu.Unlock()
}

func newAllocator(t *testing.T, clock *manualClock, epoch, previous *uint64) *emission.SequenceAllocator {
	t.Helper()
	allocator, err := emission.NewSequenceAllocator(emission.SequenceOptions{Now: clock.now, Epoch: epoch, PreviousEpoch: previous})
	if err != nil {
		t.Fatal(err)
	}
	return allocator
}

func allocate(t *testing.T, allocator *emission.SequenceAllocator) emission.Slot {
	t.Helper()
	slot, err := allocator.Allocate()
	if err != nil {
		t.Fatal(err)
	}
	return slot
}

func ptr(value uint64) *uint64 { return &value }

func sequenceCode(err error) string {
	var sequence *emission.SequenceError
	if errors.As(err, &sequence) {
		return sequence.Code
	}
	return ""
}

func TestSequenceEpochFromClockAndOverride(t *testing.T) {
	clock := &manualClock{ms: int64(epochE)}
	allocator := newAllocator(t, clock, nil, nil)
	if got := allocate(t, allocator); got != (emission.Slot{Epoch: epochE, Number: 1}) {
		t.Fatalf("first slot = %+v", got)
	}
	if got := allocate(t, allocator); got != (emission.Slot{Epoch: epochE, Number: 2}) {
		t.Fatalf("second slot = %+v", got)
	}
	if allocator.HighestAllocated() != 2 || allocator.Epoch() != epochE {
		t.Fatalf("highest %d epoch %d", allocator.HighestAllocated(), allocator.Epoch())
	}
	override := newAllocator(t, clock, ptr(42), nil)
	if got := allocate(t, override); got != (emission.Slot{Epoch: 42, Number: 1}) {
		t.Fatalf("override slot = %+v", got)
	}
}

func TestSequenceRestartContract(t *testing.T) {
	clock := &manualClock{ms: int64(epochE - 60_000)}
	if got := newAllocator(t, clock, nil, ptr(epochE)).Epoch(); got != epochE+1 {
		t.Fatalf("clock behind: epoch %d", got)
	}
	clock.set(int64(epochE + 60_000))
	if got := newAllocator(t, clock, nil, ptr(epochE)).Epoch(); got != epochE+60_000 {
		t.Fatalf("clock ahead: epoch %d", got)
	}
	for _, epoch := range []uint64{100, 99} {
		_, err := emission.NewSequenceAllocator(emission.SequenceOptions{Now: clock.now, Epoch: ptr(epoch), PreviousEpoch: ptr(100)})
		if sequenceCode(err) != emission.SequenceEpochNotGreater {
			t.Fatalf("override %d: err = %v", epoch, err)
		}
	}
	if got := newAllocator(t, clock, ptr(101), ptr(100)).Epoch(); got != 101 {
		t.Fatalf("override above previous: %d", got)
	}
}

func TestSequenceCompleteThroughIsTheContiguousExportedPrefix(t *testing.T) {
	allocator := newAllocator(t, &manualClock{ms: int64(epochE)}, nil, nil)
	one, two, three := allocate(t, allocator), allocate(t, allocator), allocate(t, allocator)
	allocator.MarkExported(three)
	expectDeclared(t, allocator, 3, 0)
	allocator.MarkExported(one)
	expectDeclared(t, allocator, 3, 1)
	allocator.MarkFailed(two)
	expectDeclared(t, allocator, 3, 1)
	allocator.MarkExported(two)
	expectDeclared(t, allocator, 3, 3)
}

func TestSequenceFilteredSlotsDoNotAdvance(t *testing.T) {
	allocator := newAllocator(t, &manualClock{ms: int64(epochE)}, nil, nil)
	slots := make([]emission.Slot, 6)
	for i := range slots {
		slots[i] = allocate(t, allocator)
	}
	allocator.MarkFiltered(slots[3], 4)
	allocator.MarkFiltered(slots[1], 4)
	allocator.MarkFiltered(slots[2], 4)
	allocator.MarkFiltered(slots[4], 5)
	allocator.MarkExported(slots[0])
	allocator.MarkExported(slots[5])
	allocator.MarkExported(slots[1]) // filtered is terminal
	declared := allocator.Declaration()
	want := []otlp.FilteredRange{{First: 2, Last: 4, FilterEpoch: 4}, {First: 5, Last: 5, FilterEpoch: 5}}
	if declared.CompleteThrough != 1 || fmt.Sprint(declared.Filtered) != fmt.Sprint(want) {
		t.Fatalf("declaration = %+v", declared)
	}
}

func TestSequenceIgnoresUnknownAndTerminalMarks(t *testing.T) {
	allocator := newAllocator(t, &manualClock{ms: int64(epochE)}, nil, nil)
	one := allocate(t, allocator)
	allocator.MarkExported(emission.Slot{Epoch: epochE, Number: 2})
	allocator.MarkExported(emission.Slot{Epoch: epochE + 1, Number: 1})
	allocator.MarkFiltered(emission.Slot{Epoch: epochE, Number: 0}, 1)
	expectDeclared(t, allocator, 1, 0)
	allocator.MarkExported(one)
	allocator.MarkFailed(one)
	allocator.MarkFiltered(one, 3)
	declared := allocator.Declaration()
	if declared.CompleteThrough != 1 || len(declared.Filtered) != 0 {
		t.Fatalf("declaration = %+v", declared)
	}
}

func TestSequenceDeclareSealsAndTheNextAllocationRolls(t *testing.T) {
	clock := &manualClock{ms: int64(epochE)}
	allocator := newAllocator(t, clock, nil, nil)
	allocator.MarkExported(allocate(t, allocator))
	allocator.Declaration() // read-only
	if got := allocate(t, allocator); got.Epoch != epochE || got.Number != 2 {
		t.Fatalf("declaration() sealed: %+v", got)
	}
	allocator.MarkExported(emission.Slot{Epoch: epochE, Number: 2})
	if declared := allocator.Declare(); declared.LastEmitted != 2 || declared.CompleteThrough != 2 || declared.Previous != nil {
		t.Fatalf("declare = %+v", declared)
	}
	if allocator.Epoch() != epochE {
		t.Fatal("the roll is lazy")
	}
	if got := allocate(t, allocator); got != (emission.Slot{Epoch: epochE + 1, Number: 1}) {
		t.Fatalf("rolled slot = %+v", got)
	}
	declared := allocator.Declaration()
	if declared.Previous == nil || *declared.Previous != (otlp.EpochDeclaration{Epoch: epochE, LastEmitted: 2, CompleteThrough: 2}) {
		t.Fatalf("previous = %+v", declared.Previous)
	}
	clock.set(int64(epochE + 60_000))
	allocator.MarkExported(emission.Slot{Epoch: epochE + 1, Number: 1})
	allocator.Declare()
	if got := allocate(t, allocator); got.Epoch != epochE+60_000 {
		t.Fatalf("roll takes the clock when ahead: %+v", got)
	}
}

func TestSequenceOpenDeclareDoesNotSealButAFreshEpochDoes(t *testing.T) {
	allocator := newAllocator(t, &manualClock{ms: int64(epochE)}, nil, nil)
	allocate(t, allocator)
	allocator.Declare()
	if got := allocate(t, allocator); got.Epoch != epochE || got.Number != 2 {
		t.Fatalf("open declare sealed: %+v", got)
	}
	fresh := newAllocator(t, &manualClock{ms: int64(epochE)}, nil, nil)
	if declared := fresh.Declare(); declared.LastEmitted != 0 || declared.CompleteThrough != 0 {
		t.Fatalf("fresh = %+v", declared)
	}
	if got := allocate(t, fresh); got.Epoch != epochE+1 {
		t.Fatalf("(0, 0) did not seal: %+v", got)
	}
}

func TestSequenceRefusesToWrap(t *testing.T) {
	allocator := newAllocator(t, &manualClock{ms: int64(epochE)}, ptr(emission.SequenceMax), nil)
	allocator.Declare()
	if _, err := allocator.Allocate(); sequenceCode(err) != emission.SequenceOverflow {
		t.Fatalf("err = %v", err)
	}
	if _, err := emission.NewSequenceAllocator(emission.SequenceOptions{Epoch: ptr(emission.SequenceMax + 1)}); sequenceCode(err) != emission.SequenceOverflow {
		t.Fatalf("err = %v", err)
	}
}

// N workers allocate and export while one declares in a loop. Every slot is
// unique, and no slot belongs to an epoch whose close was declared earlier.
func TestSequenceConcurrentAllocateAndDeclare(t *testing.T) {
	allocator := newAllocator(t, &manualClock{ms: int64(epochE)}, nil, nil)
	const workers, perWorker = 8, 400
	var closedMu sync.Mutex
	closed := map[uint64]bool{}
	stop := make(chan struct{})
	declarer := make(chan struct{})
	go func() {
		defer close(declarer)
		for {
			select {
			case <-stop:
				return
			default:
			}
			declared := allocator.Declare()
			if declared.CompleteThrough == declared.LastEmitted {
				closedMu.Lock()
				closed[declared.Epoch] = true
				closedMu.Unlock()
			}
		}
	}()
	results := make(chan emission.Slot, workers*perWorker)
	var wg sync.WaitGroup
	for range workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range perWorker {
				closedMu.Lock()
				snapshot := make(map[uint64]bool, len(closed))
				for epoch := range closed {
					snapshot[epoch] = true
				}
				closedMu.Unlock()
				slot, err := allocator.Allocate()
				if err != nil {
					t.Error(err)
					return
				}
				if snapshot[slot.Epoch] {
					t.Errorf("slot %+v allocated in an epoch already declared closed", slot)
				}
				allocator.MarkExported(slot)
				results <- slot
			}
		}()
	}
	wg.Wait()
	close(stop)
	<-declarer
	close(results)
	seen := map[emission.Slot]bool{}
	for slot := range results {
		if seen[slot] {
			t.Fatalf("duplicate slot %+v", slot)
		}
		seen[slot] = true
	}
	if len(seen) != workers*perWorker {
		t.Fatalf("slots = %d", len(seen))
	}
}

func expectDeclared(t *testing.T, allocator *emission.SequenceAllocator, last, complete uint64) {
	t.Helper()
	declared := allocator.Declaration()
	if declared.LastEmitted != last || declared.CompleteThrough != complete {
		t.Fatalf("declared (%d, %d), want (%d, %d)", declared.LastEmitted, declared.CompleteThrough, last, complete)
	}
}

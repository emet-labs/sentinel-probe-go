package emission

import (
	"cmp"
	"context"
	"fmt"
	"math"
	"slices"
	"sync"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"

	"github.com/emet-labs/sentinel/sdk/go/otlp"
)

// SequenceMax is the largest Sequence Epoch or sequence number: OTel
// attributes carry them as int64.
const SequenceMax = uint64(math.MaxInt64)

// Sequence error codes.
const (
	// SequenceEpochNotGreater: an epoch override is not above PreviousEpoch.
	SequenceEpochNotGreater = "epoch-not-greater"
	// SequenceOverflow: allocation past SequenceMax. Numbers never wrap.
	SequenceOverflow = "sequence-overflow"
)

// SequenceError is a refused allocator construction or allocation.
type SequenceError struct {
	Code   string
	Detail string
}

func (e *SequenceError) Error() string {
	return fmt.Sprintf("emission: sequence: %s: %s", e.Code, e.Detail)
}

// Slot is one allocated (Sequence Epoch, sequence number).
type Slot struct {
	Epoch, Number uint64
}

// SequenceOptions configures a SequenceAllocator.
type SequenceOptions struct {
	// Now defaults to time.Now. Injected so no unit test reads a wall clock.
	Now func() time.Time
	// Epoch is a host-chosen first epoch. Nil derives it from Now's Unix milliseconds.
	Epoch *uint64
	// PreviousEpoch is the last epoch of the previous run, if the host
	// persisted it. The new epoch is then strictly greater even when the
	// clock stepped back. Without it a backward clock step across a restart
	// cannot be detected, and Sentinel refuses the regressed epoch.
	PreviousEpoch *uint64
}

type slotState uint8

const (
	slotInFlight slotState = iota + 1
	slotExported
	slotFailed
	slotFiltered
)

// SequenceAllocator hands out sequence slots for one source and tracks what
// became of each one (#461).
//
// complete_through never runs ahead of a successful export: it advances only
// over exported slots, and an in-flight, failed or filtered slot blocks it. A
// slot the Event Filter dropped is filtered and does not advance
// complete_through until #542; its range is kept in the declaration.
//
// A declared close (complete_through == last_emitted) seals the epoch:
// Sentinel refuses an epoch that advances after its close. The next
// allocation therefore opens max(epoch + 1, clock Unix ms). The roll is lazy,
// so an idle source keeps its closed epoch until it emits again. One mutex
// guards everything, so an allocation and a Declare cannot interleave.
type SequenceAllocator struct {
	now func() time.Time

	mu              sync.Mutex
	epoch           uint64
	last            uint64
	completeThrough uint64
	sealed          bool
	states          map[uint64]slotState
	filtered        map[uint64]uint64
	previous        *otlp.EpochDeclaration
}

var _ otlp.DeclarationSource = (*SequenceAllocator)(nil)

// NewSequenceAllocator builds an allocator. An Epoch override that is not
// above PreviousEpoch is refused with SequenceEpochNotGreater.
func NewSequenceAllocator(options SequenceOptions) (*SequenceAllocator, error) {
	now := options.Now
	if now == nil {
		now = time.Now
	}
	var epoch uint64
	switch {
	case options.Epoch != nil:
		epoch = *options.Epoch
		if options.PreviousEpoch != nil && epoch <= *options.PreviousEpoch {
			return nil, &SequenceError{Code: SequenceEpochNotGreater,
				Detail: fmt.Sprintf("epoch %d is not greater than the previous epoch %d", epoch, *options.PreviousEpoch)}
		}
	case options.PreviousEpoch != nil:
		next, err := successor(*options.PreviousEpoch)
		if err != nil {
			return nil, err
		}
		epoch = max(clockMillis(now), next)
	default:
		epoch = clockMillis(now)
	}
	if epoch > SequenceMax {
		return nil, &SequenceError{Code: SequenceOverflow, Detail: fmt.Sprintf("epoch %d is above %d", epoch, SequenceMax)}
	}
	return &SequenceAllocator{
		now:      now,
		epoch:    epoch,
		states:   map[uint64]slotState{},
		filtered: map[uint64]uint64{},
	}, nil
}

// Allocate returns the next slot, first opening a new epoch if the current
// one is sealed.
func (a *SequenceAllocator) Allocate() (Slot, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.sealed {
		next, err := successor(a.epoch)
		if err != nil {
			return Slot{}, err
		}
		a.previous = &otlp.EpochDeclaration{Epoch: a.epoch, LastEmitted: a.last, CompleteThrough: a.completeThrough}
		a.epoch = max(next, clockMillis(a.now))
		a.last, a.completeThrough, a.sealed = 0, 0, false
		a.states = map[uint64]slotState{}
		a.filtered = map[uint64]uint64{}
	}
	if a.last >= SequenceMax {
		return Slot{}, &SequenceError{Code: SequenceOverflow, Detail: fmt.Sprintf("sequence numbers in epoch %d are exhausted", a.epoch)}
	}
	a.last++
	a.states[a.last] = slotInFlight
	return Slot{Epoch: a.epoch, Number: a.last}, nil
}

// pending is the state of a slot that can still change; a.mu is held.
func (a *SequenceAllocator) pending(slot Slot) slotState {
	if slot.Epoch != a.epoch || slot.Number <= a.completeThrough || slot.Number > a.last {
		return 0
	}
	return a.states[slot.Number]
}

// MarkExported records that the slot's Event reached Sentinel: HTTP 2xx and
// zero rejected spans. A failed slot may still be marked exported.
func (a *SequenceAllocator) MarkExported(slot Slot) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if state := a.pending(slot); state == slotInFlight || state == slotFailed {
		a.states[slot.Number] = slotExported
		for a.states[a.completeThrough+1] == slotExported {
			a.completeThrough++
			delete(a.states, a.completeThrough)
		}
	}
}

// MarkFailed records that the slot's export failed. It is not retried here.
func (a *SequenceAllocator) MarkFailed(slot Slot) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.pending(slot) == slotInFlight {
		a.states[slot.Number] = slotFailed
	}
}

// MarkFiltered records that the Event Filter dropped the slot's Event under filterEpoch.
func (a *SequenceAllocator) MarkFiltered(slot Slot, filterEpoch uint64) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.pending(slot) == slotInFlight {
		a.states[slot.Number] = slotFiltered
		a.filtered[slot.Number] = filterEpoch
	}
}

// Epoch is the current Sequence Epoch.
func (a *SequenceAllocator) Epoch() uint64 {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.epoch
}

// HighestAllocated is the highest number allocated in the current epoch.
func (a *SequenceAllocator) HighestAllocated() uint64 {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.last
}

// Declaration is a read-only snapshot.
func (a *SequenceAllocator) Declaration() otlp.SequenceDeclaration {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.declaration()
}

// Declare is a snapshot for a completeness signal. It seals the epoch when
// the snapshot is a close. Only a heartbeat should call it.
func (a *SequenceAllocator) Declare() otlp.SequenceDeclaration {
	a.mu.Lock()
	defer a.mu.Unlock()
	declared := a.declaration()
	if a.completeThrough == a.last {
		a.sealed = true
	}
	return declared
}

func (a *SequenceAllocator) declaration() otlp.SequenceDeclaration {
	declared := otlp.SequenceDeclaration{
		EpochDeclaration: otlp.EpochDeclaration{Epoch: a.epoch, LastEmitted: a.last, CompleteThrough: a.completeThrough},
		Filtered:         filteredRanges(a.filtered),
	}
	if a.previous != nil {
		previous := *a.previous
		declared.Previous = &previous
	}
	return declared
}

func filteredRanges(slots map[uint64]uint64) []otlp.FilteredRange {
	numbers := make([]uint64, 0, len(slots))
	for number := range slots {
		numbers = append(numbers, number)
	}
	slices.SortFunc(numbers, cmp.Compare[uint64])
	ranges := []otlp.FilteredRange{}
	for _, number := range numbers {
		filterEpoch := slots[number]
		if n := len(ranges); n > 0 && ranges[n-1].Last+1 == number && ranges[n-1].FilterEpoch == filterEpoch {
			ranges[n-1].Last = number
			continue
		}
		ranges = append(ranges, otlp.FilteredRange{First: number, Last: number, FilterEpoch: filterEpoch})
	}
	return ranges
}

func clockMillis(now func() time.Time) uint64 {
	ms := now().UnixMilli()
	if ms < 0 {
		return 0
	}
	return uint64(ms)
}

func successor(epoch uint64) (uint64, error) {
	if epoch >= SequenceMax {
		return 0, &SequenceError{Code: SequenceOverflow, Detail: fmt.Sprintf("epoch %d has no successor", epoch)}
	}
	return epoch + 1, nil
}

// sequenceStamper allocates a slot when a span starts, unless the span
// already carries both sequence attributes (a host-reserved slot).
type sequenceStamper struct {
	allocator *SequenceAllocator
}

func (s sequenceStamper) OnStart(_ context.Context, span sdktrace.ReadWriteSpan) {
	// A panic here would reach the host's tracer.Start; the span then exports without a slot and fails.
	defer func() {
		if r := recover(); r != nil {
			otel.Handle(fmt.Errorf("emission: sequence stamper: panic: %v", r))
		}
	}()
	if hasBothSequenceKeys(span.Attributes()) {
		return
	}
	slot, err := s.allocator.Allocate()
	if err != nil {
		otel.Handle(err)
		return
	}
	span.SetAttributes(
		attribute.Int64(otlp.AttributeSequenceEpoch, int64(slot.Epoch)),
		attribute.Int64(otlp.AttributeSequenceNumber, int64(slot.Number)),
	)
}

func (sequenceStamper) OnEnd(sdktrace.ReadOnlySpan)      {}
func (sequenceStamper) Shutdown(context.Context) error   { return nil }
func (sequenceStamper) ForceFlush(context.Context) error { return nil }

func slotOf(span sdktrace.ReadOnlySpan) (Slot, bool) {
	var epoch, number int64
	var haveEpoch, haveNumber bool
	for _, kv := range span.Attributes() {
		if kv.Value.Type() != attribute.INT64 {
			continue
		}
		switch kv.Key {
		case otlp.AttributeSequenceEpoch:
			epoch, haveEpoch = kv.Value.AsInt64(), true
		case otlp.AttributeSequenceNumber:
			number, haveNumber = kv.Value.AsInt64(), true
		}
	}
	if !haveEpoch || !haveNumber || epoch < 0 || number < 0 {
		return Slot{}, false
	}
	return Slot{Epoch: uint64(epoch), Number: uint64(number)}, true
}

func hasBothSequenceKeys(attributes []attribute.KeyValue) bool {
	var epoch, number bool
	for _, kv := range attributes {
		switch kv.Key {
		case otlp.AttributeSequenceEpoch:
			epoch = true
		case otlp.AttributeSequenceNumber:
			number = true
		}
	}
	return epoch && number
}

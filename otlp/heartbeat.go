package otlp

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	modelv1 "github.com/emet-labs/sentinel-probe-go/gen/sentinel/model/v1"
	"github.com/emet-labs/sentinel-probe-go/int128"
)

// UnixClockDomain is the producer clock domain every heartbeat signal names.
const UnixClockDomain = "unix"

// DefaultHeartbeatInterval is the interval used when HeartbeatConfig.Interval
// is zero. Sentinel's active-silence bound is 60 s.
const DefaultHeartbeatInterval = 15 * time.Second

// Heartbeat error codes reported through HeartbeatConfig.OnError.
const (
	// HeartbeatFilterEpochRegressed: the acknowledged Filter epoch went below
	// one already sent for the current Sequence Epoch, so nothing was sent.
	HeartbeatFilterEpochRegressed = "filter-epoch-regressed"
	// HeartbeatNotAccepted: Sentinel refused a signal or the send failed.
	HeartbeatNotAccepted = "not-accepted"
)

// FilteredRange is a run of contiguous slots the Event Filter dropped under
// one Filter epoch.
type FilteredRange struct {
	First, Last, FilterEpoch uint64
}

// EpochDeclaration is what one Sequence Epoch declares.
type EpochDeclaration struct {
	Epoch, LastEmitted, CompleteThrough uint64
}

// SequenceDeclaration is the current epoch's declaration, the slots the
// Event Filter dropped in it, and the final declaration of the epoch before it.
type SequenceDeclaration struct {
	EpochDeclaration
	Filtered []FilteredRange
	Previous *EpochDeclaration
}

// DeclarationSource is what a heartbeat declares from. An
// *emission.SequenceAllocator is one.
type DeclarationSource interface {
	// Declaration is a read-only snapshot.
	Declaration() SequenceDeclaration
	// Declare is a snapshot for a signal; it seals the epoch when it is a close.
	Declare() SequenceDeclaration
}

// CompletenessReport is one signal a report sent and whether Sentinel accepted it.
type CompletenessReport struct {
	SequenceEpoch           uint64
	LastEmitted             uint64
	CompleteThrough         uint64
	AcknowledgedFilterEpoch uint64
	Accepted                bool
	// Err is the send error when the signal was not accepted.
	Err    error
	Signal *modelv1.ProducerCompletenessSignal
}

// HeartbeatError is a refused signal or a regressed Filter epoch.
type HeartbeatError struct {
	Code string
	Err  error
}

func (e *HeartbeatError) Error() string {
	if e.Err == nil {
		return "otlp: heartbeat: " + e.Code
	}
	return fmt.Sprintf("otlp: heartbeat: %s: %v", e.Code, e.Err)
}

func (e *HeartbeatError) Unwrap() error { return e.Err }

// HeartbeatConfig configures a CompletenessHeartbeat. Clock, scheduler and
// send function are injectable so no unit test reads a wall clock.
type HeartbeatConfig struct {
	// Source is the source's allocator. Required.
	Source DeclarationSource
	// FilterEpoch returns the acknowledged Filter epoch, or nil when no Filter
	// is held, which sends 0. (*client.FilterStore).Epoch fits.
	FilterEpoch func() *uint64
	// Send delivers one signal; a nil error means Sentinel accepted it. When
	// nil, ExportCompleteness posts to Endpoint/v1/logs as SourceHandle.
	Send         func(context.Context, *modelv1.ProducerCompletenessSignal) error
	Endpoint     string
	SourceHandle string
	// Now defaults to time.Now.
	Now func() time.Time
	// Interval between reports, 1 s to 59.999 s. Zero means
	// DefaultHeartbeatInterval.
	Interval time.Duration
	// Manual schedules nothing: Run only waits, and reports happen on Report.
	Manual bool
	// Ticks defaults to a time.Ticker. It returns the tick channel and a stop function.
	Ticks func(time.Duration) (<-chan time.Time, func())
	// OnError receives each *HeartbeatError. Defaults to discarding them.
	OnError func(error)
}

type accepted struct {
	epoch, lastEmitted, completeThrough, ack uint64
}

type reportBatch struct {
	ctx     context.Context
	start   chan struct{}
	done    chan struct{}
	waiters int
	reports []CompletenessReport
}

// CompletenessHeartbeat sends periodic producer completeness for one source (#461).
//
// Each report declares the allocator's epoch, last_emitted and
// complete_through. Between reports with no change every field but
// producer_reported_at is identical, so Sentinel treats the heartbeat as a
// restatement and writes no Verdict revision. producer_reported_at never goes
// backwards and never precedes activity_since, even when the clock steps back.
//
// Before the first accepted signal of a new epoch, the previous epoch's final
// declaration is restated once per report unless Sentinel already accepted it
// under the current acknowledged Filter epoch. A heartbeat never regresses an
// acknowledged Filter epoch within one Sequence Epoch: it sends nothing and
// reports HeartbeatFilterEpochRegressed.
type CompletenessHeartbeat struct {
	config   HeartbeatConfig
	interval time.Duration

	mu             sync.Mutex
	idle           bool
	activitySince  int64
	idleMark       EpochDeclaration
	lastReportedAt int64
	lastAccepted   *accepted
	ackFloor       map[uint64]uint64
	stopped        bool

	coalesce sync.Mutex
	running  bool
	queued   *reportBatch

	quitOnce sync.Once
	quit     chan struct{}
}

// NewCompletenessHeartbeat validates config and builds a heartbeat. The
// activity is ACTIVE since the construction instant.
func NewCompletenessHeartbeat(config HeartbeatConfig) (*CompletenessHeartbeat, error) {
	if config.Source == nil {
		return nil, refuse("missing-declaration-source", "")
	}
	if config.Send == nil {
		if strings.TrimSpace(config.SourceHandle) == "" {
			return nil, refuse("missing-source-handle", "")
		}
		if _, err := endpointURL(config.Endpoint, "/v1/logs"); err != nil {
			return nil, err
		}
		endpoint, handle := config.Endpoint, config.SourceHandle
		config.Send = func(ctx context.Context, signal *modelv1.ProducerCompletenessSignal) error {
			return ExportCompleteness(ctx, endpoint, handle, signal)
		}
	}
	interval := config.Interval
	if interval == 0 {
		interval = DefaultHeartbeatInterval
	}
	if interval < time.Second || interval >= time.Minute {
		return nil, refuse("invalid-heartbeat-interval", interval.String())
	}
	if config.Now == nil {
		config.Now = time.Now
	}
	if config.Ticks == nil {
		config.Ticks = func(every time.Duration) (<-chan time.Time, func()) {
			ticker := time.NewTicker(every)
			return ticker.C, ticker.Stop
		}
	}
	if config.OnError == nil {
		config.OnError = func(error) {}
	}
	return &CompletenessHeartbeat{
		config:        config,
		interval:      interval,
		activitySince: config.Now().UnixNano(),
		ackFloor:      map[uint64]uint64{},
		quit:          make(chan struct{}),
	}, nil
}

// Run reports every interval until ctx is cancelled, Stop or Shutdown is
// called. Cancelling ctx only ends the schedule; Shutdown can still send a
// final report. With Manual, Run schedules nothing and only waits.
func (h *CompletenessHeartbeat) Run(ctx context.Context) error {
	if h.config.Manual {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-h.quit:
			return nil
		}
	}
	ticks, stop := h.config.Ticks(h.interval)
	defer stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-h.quit:
			return nil
		case <-ticks:
			select {
			case <-h.quit:
				return nil
			default:
			}
			h.Report(ctx)
		}
	}
}

// Report declares and sends. Reports run one at a time; a call made during a
// report causes exactly one more report after it, built when it starts, and
// every caller that joined it receives its reports.
func (h *CompletenessHeartbeat) Report(ctx context.Context) []CompletenessReport {
	h.coalesce.Lock()
	if !h.running {
		h.running = true
		batch := &reportBatch{ctx: ctx, done: make(chan struct{})}
		h.coalesce.Unlock()
		h.drive(batch)
		return batch.reports
	}
	if h.queued != nil {
		batch := h.queued
		batch.waiters++
		h.coalesce.Unlock()
		select {
		case <-batch.done:
			return batch.reports
		case <-ctx.Done():
			return nil
		}
	}
	batch := &reportBatch{ctx: ctx, start: make(chan struct{}), done: make(chan struct{}), waiters: 1}
	h.queued = batch
	h.coalesce.Unlock()
	// The caller that queued the follow-up runs it once the running report hands off.
	<-batch.start
	h.drive(batch)
	return batch.reports
}

func (h *CompletenessHeartbeat) drive(batch *reportBatch) {
	batch.reports = h.reportOnce(batch.ctx)
	close(batch.done)
	h.coalesce.Lock()
	next := h.queued
	h.queued = nil
	if next == nil {
		h.running = false
	}
	h.coalesce.Unlock()
	if next != nil {
		close(next.start)
	}
}

func (h *CompletenessHeartbeat) reportOnce(ctx context.Context) []CompletenessReport {
	h.mu.Lock()
	if h.stopped {
		h.mu.Unlock()
		return nil
	}
	declared := h.config.Source.Declare()
	ack := uint64(0)
	if epoch := h.config.FilterEpoch; epoch != nil {
		if value := epoch(); value != nil {
			ack = *value
		}
	}
	now := max(h.config.Now().UnixNano(), h.lastReportedAt, h.activitySince)
	h.lastReportedAt = now
	if h.idle && h.idleMark != declaredMark(declared) {
		h.idle = false
		h.activitySince = now
	}
	var plan []EpochDeclaration
	previous := declared.Previous
	acceptedCurrent := h.lastAccepted != nil && h.lastAccepted.epoch == declared.Epoch
	if previous != nil && !acceptedCurrent &&
		(h.lastAccepted == nil || *h.lastAccepted != (accepted{previous.Epoch, previous.LastEmitted, previous.CompleteThrough, ack})) &&
		ack >= h.ackFloor[previous.Epoch] {
		plan = append(plan, *previous)
	}
	regressed := ack < h.ackFloor[declared.Epoch]
	if !regressed {
		plan = append(plan, declared.EpochDeclaration)
	}
	signals := make([]*modelv1.ProducerCompletenessSignal, len(plan))
	for i, epoch := range plan {
		signals[i] = h.signal(epoch, ack, now)
	}
	h.mu.Unlock()

	if regressed {
		h.config.OnError(&HeartbeatError{
			Code: HeartbeatFilterEpochRegressed,
			Err:  fmt.Errorf("acknowledged Filter epoch %d is below one already sent for epoch %d", ack, declared.Epoch),
		})
	}
	reports := make([]CompletenessReport, 0, len(plan))
	for i, epoch := range plan {
		h.mu.Lock()
		stopped := h.stopped
		h.mu.Unlock()
		if stopped {
			break
		}
		err := h.config.Send(ctx, signals[i])
		h.mu.Lock()
		// Raised whatever the outcome: a timed-out send may still have landed.
		h.ackFloor[epoch.Epoch] = max(h.ackFloor[epoch.Epoch], ack)
		if err == nil {
			h.lastAccepted = &accepted{epoch.Epoch, epoch.LastEmitted, epoch.CompleteThrough, ack}
		}
		h.mu.Unlock()
		if err != nil {
			h.config.OnError(&HeartbeatError{Code: HeartbeatNotAccepted, Err: err})
		}
		reports = append(reports, CompletenessReport{
			SequenceEpoch:           epoch.Epoch,
			LastEmitted:             epoch.LastEmitted,
			CompleteThrough:         epoch.CompleteThrough,
			AcknowledgedFilterEpoch: ack,
			Accepted:                err == nil,
			Err:                     err,
			Signal:                  signals[i],
		})
	}
	h.mu.Lock()
	// Only the current epoch and its predecessor can be sent again.
	for epoch := range h.ackFloor {
		if epoch != declared.Epoch && (previous == nil || epoch != previous.Epoch) {
			delete(h.ackFloor, epoch)
		}
	}
	h.mu.Unlock()
	return reports
}

// signal builds one signal; h.mu is held.
func (h *CompletenessHeartbeat) signal(declared EpochDeclaration, ack uint64, now int64) *modelv1.ProducerCompletenessSignal {
	activity := modelv1.SourceActivityState_SOURCE_ACTIVITY_STATE_ACTIVE
	if h.idle {
		activity = modelv1.SourceActivityState_SOURCE_ACTIVITY_STATE_INTENTIONALLY_IDLE
	}
	return BuildCompletenessSignal(CompletenessFields{
		SequenceEpoch:           declared.Epoch,
		LastEmitted:             declared.LastEmitted,
		CompleteThrough:         declared.CompleteThrough,
		AcknowledgedFilterEpoch: ack,
		Activity:                activity,
		ReportedAtNanos:         now,
		ActivitySinceNanos:      h.activitySince,
	})
}

func declaredMark(declared SequenceDeclaration) EpochDeclaration {
	return EpochDeclaration{Epoch: declared.Epoch, LastEmitted: declared.LastEmitted}
}

// DeclareIdle marks the source intentionally idle. The next allocation ends it.
func (h *CompletenessHeartbeat) DeclareIdle() {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.idle {
		return
	}
	h.idle = true
	h.activitySince = max(h.config.Now().UnixNano(), h.lastReportedAt)
	h.idleMark = declaredMark(h.config.Source.Declaration())
}

// DeclareActive ends an intentional idle.
func (h *CompletenessHeartbeat) DeclareActive() {
	h.mu.Lock()
	defer h.mu.Unlock()
	if !h.idle {
		return
	}
	h.idle = false
	h.activitySince = max(h.config.Now().UnixNano(), h.lastReportedAt)
}

// Stop makes the source go silent: it ends Run and sends nothing more.
func (h *CompletenessHeartbeat) Stop() {
	h.quitOnce.Do(func() { close(h.quit) })
	h.mu.Lock()
	h.stopped = true
	h.mu.Unlock()
}

// Shutdown ends Run, waits for a running report, sends one final report and
// then stops. The final signal is a close only if nothing is unresolved.
func (h *CompletenessHeartbeat) Shutdown(ctx context.Context) []CompletenessReport {
	h.quitOnce.Do(func() { close(h.quit) })
	reports := h.Report(ctx)
	h.mu.Lock()
	h.stopped = true
	h.mu.Unlock()
	return reports
}

// CompletenessFields is every field of one producer completeness signal.
type CompletenessFields struct {
	SequenceEpoch           uint64
	LastEmitted             uint64
	CompleteThrough         uint64
	AcknowledgedFilterEpoch uint64
	Activity                modelv1.SourceActivityState
	ReportedAtNanos         int64
	ActivitySinceNanos      int64
}

// BuildCompletenessSignal builds a ProducerCompletenessSignal with
// last_emitted, complete_through and acknowledged_filter_epoch present even
// when zero, both times in the unix clock domain with zero uncertainty.
func BuildCompletenessSignal(fields CompletenessFields) *modelv1.ProducerCompletenessSignal {
	lastEmitted, completeThrough, ack := fields.LastEmitted, fields.CompleteThrough, fields.AcknowledgedFilterEpoch
	return &modelv1.ProducerCompletenessSignal{
		SequenceEpoch:           fields.SequenceEpoch,
		LastEmitted:             &lastEmitted,
		CompleteThrough:         &completeThrough,
		ProducerReportedAt:      unixTime(fields.ReportedAtNanos),
		AcknowledgedFilterEpoch: &ack,
		Activity:                fields.Activity,
		ActivitySince:           unixTime(fields.ActivitySinceNanos),
	}
}

func unixTime(nanoseconds int64) *modelv1.OccurrenceTime {
	return &modelv1.OccurrenceTime{
		ClockDomainId:          UnixClockDomain,
		Nanoseconds:            int128.FromInt64(nanoseconds),
		UncertaintyNanoseconds: 0,
	}
}

// IsHeartbeatError reports whether err is a *HeartbeatError with code.
func IsHeartbeatError(err error, code string) bool {
	var heartbeat *HeartbeatError
	return errors.As(err, &heartbeat) && heartbeat.Code == code
}

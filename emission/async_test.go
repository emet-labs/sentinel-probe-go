package emission_test

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	coltracepb "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	"google.golang.org/protobuf/proto"

	"github.com/emet-labs/sentinel/sdk/go/emission"
	"github.com/emet-labs/sentinel/sdk/go/otlp"
)

// sentinelStub is a Sentinel that answers each trace export with handler's reply and records what it saw.
type sentinelStub struct {
	URL      string
	mu       sync.Mutex
	requests int
	spans    []*coltracepb.ExportTraceServiceRequest
}

type stubReply struct {
	status   int
	body     string
	rejected int64
	message  string
}

func newStub(t *testing.T, handler func(n int) stubReply) *sentinelStub {
	t.Helper()
	stub := &sentinelStub{}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		raw, _ := io.ReadAll(r.Body)
		var request coltracepb.ExportTraceServiceRequest
		_ = proto.Unmarshal(raw, &request)
		stub.mu.Lock()
		stub.requests++
		n := stub.requests
		stub.spans = append(stub.spans, &request)
		stub.mu.Unlock()
		reply := handler(n)
		if reply.status == 0 {
			reply.status = http.StatusOK
		}
		body := []byte(reply.body)
		if reply.rejected != 0 || reply.message != "" {
			body, _ = proto.Marshal(&coltracepb.ExportTraceServiceResponse{PartialSuccess: &coltracepb.ExportTracePartialSuccess{
				RejectedSpans: reply.rejected, ErrorMessage: reply.message,
			}})
		}
		w.Header().Set("Content-Type", "application/x-protobuf")
		w.WriteHeader(reply.status)
		_, _ = w.Write(body)
	}))
	t.Cleanup(server.Close)
	stub.URL = server.URL
	return stub
}

func (s *sentinelStub) count() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.requests
}

func (s *sentinelStub) eventIDs() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	var ids []string
	for _, request := range s.spans {
		for _, rs := range request.GetResourceSpans() {
			for _, ss := range rs.GetScopeSpans() {
				for _, span := range ss.GetSpans() {
					for _, kv := range span.GetAttributes() {
						if kv.GetKey() == "sentinel.event.id" {
							ids = append(ids, kv.GetValue().GetStringValue())
						}
					}
				}
			}
		}
	}
	return ids
}

type errorLog struct {
	mu    sync.Mutex
	codes []string
	errs  []*emission.ExportError
}

func (l *errorLog) record(err error) {
	var exportErr *emission.ExportError
	if e, ok := err.(*emission.ExportError); ok {
		exportErr = e
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	l.errs = append(l.errs, exportErr)
	l.codes = append(l.codes, exportErr.Code)
}

func (l *errorLog) has(code string) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	for _, c := range l.codes {
		if c == code {
			return true
		}
	}
	return false
}

type asyncRig struct {
	tracer    *emission.ProbeTracer
	allocator *emission.SequenceAllocator
	errors    *errorLog
	sleeps    *[]time.Duration
}

func newRig(t *testing.T, endpoint string, async emission.AsyncOptions) asyncRig {
	t.Helper()
	allocator, err := emission.NewSequenceAllocator(emission.SequenceOptions{})
	if err != nil {
		t.Fatal(err)
	}
	var sleepMu sync.Mutex
	sleeps := []time.Duration{}
	if async.Sleep == nil {
		async.Sleep = func(ctx context.Context, d time.Duration) error {
			sleepMu.Lock()
			sleeps = append(sleeps, d)
			sleepMu.Unlock()
			return ctx.Err()
		}
	}
	log := &errorLog{}
	tracer := emission.NewProbeTracer(emission.Options{
		TracerName:   "probe.test",
		Endpoint:     endpoint,
		SourceHandle: "shop-payment",
		Sequence:     allocator,
		Async:        async,
		OnError:      log.record,
		Logf:         func(string, ...any) {},
		HostStamp: &emission.HostStamperOptions{
			SourceHandle: "shop-payment", SchemaVersion: "sagashop-contract-v1",
		},
	})
	t.Cleanup(func() { _ = tracer.Shutdown(context.Background()) })
	return asyncRig{tracer: tracer, allocator: allocator, errors: log, sleeps: &sleeps}
}

func (r asyncRig) emit(id string, extra ...attribute.KeyValue) {
	attrs := append([]attribute.KeyValue{attribute.String(emission.AttributeEventID, id)}, extra...)
	_, span := r.tracer.Tracer.Start(context.Background(), "shop.payment.charge", trace.WithAttributes(attrs...))
	span.End()
}

func (r asyncRig) flush(t *testing.T) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := r.tracer.Provider.ForceFlush(ctx); err != nil {
		t.Fatal(err)
	}
}

func (r asyncRig) declared() (last, complete uint64) {
	d := r.allocator.Declaration()
	return d.LastEmitted, d.CompleteThrough
}

func expectSlots(t *testing.T, r asyncRig, last, complete uint64) {
	t.Helper()
	if l, c := r.declared(); l != last || c != complete {
		t.Fatalf("declared (last_emitted %d, complete_through %d), want (%d, %d)", l, c, last, complete)
	}
}

func TestAsyncExportMarksSlotsExportedAfterFlush(t *testing.T) {
	stub := newStub(t, func(int) stubReply { return stubReply{} })
	rig := newRig(t, stub.URL, emission.AsyncOptions{})
	for _, id := range []string{"a", "b", "c"} {
		rig.emit(id)
	}
	rig.flush(t)
	expectSlots(t, rig, 3, 3)
	if got := len(stub.eventIDs()); got != 3 {
		t.Fatalf("Sentinel saw %d Events", got)
	}
}

// An unreachable Sentinel must not panic, return an error or block out of span.End(): the slot is FAILED, the
// failure is reported, and complete_through does not pass it.
func TestUnreachableSentinelNeverBlocksOrPanicsSpanEnd(t *testing.T) {
	server := httptest.NewServer(http.NotFoundHandler())
	endpoint := server.URL
	server.Close() // nothing listens any more
	rig := newRig(t, endpoint, emission.AsyncOptions{MaxAttempts: 3})
	started := time.Now()
	for i := 0; i < 20; i++ {
		rig.emit("e" + string(rune('a'+i)))
	}
	if elapsed := time.Since(started); elapsed > time.Second {
		t.Fatalf("span.End() blocked for %v", elapsed)
	}
	rig.flush(t)
	if !rig.errors.has(emission.ExportTransportExhausted) {
		t.Fatalf("no transport-exhausted report: %v", rig.errors.codes)
	}
	expectSlots(t, rig, 20, 0)
	if rig.tracer.Exporter.Stats().Retained == 0 {
		t.Fatal("failed spans are not kept for a later retry")
	}
}

func TestSlowSentinelDoesNotDelaySpanEnd(t *testing.T) {
	release := make(chan struct{})
	stub := newStub(t, func(int) stubReply { <-release; return stubReply{} })
	rig := newRig(t, stub.URL, emission.AsyncOptions{})
	started := time.Now()
	rig.emit("a")
	rig.emit("b")
	if elapsed := time.Since(started); elapsed > time.Second {
		t.Fatalf("span.End() waited %v for Sentinel", elapsed)
	}
	expectSlots(t, rig, 2, 0) // in flight, never complete
	close(release)
	rig.flush(t)
	expectSlots(t, rig, 2, 2)
}

func TestQueueOverflowDropsAndMarksSlotFailed(t *testing.T) {
	release := make(chan struct{})
	stub := newStub(t, func(int) stubReply { <-release; return stubReply{} })
	rig := newRig(t, stub.URL, emission.AsyncOptions{QueueSize: 2, BatchSize: 1})
	// The worker takes the first span and blocks on Sentinel; two more fill the queue; the rest overflow.
	rig.emit("e0")
	deadline := time.Now().Add(2 * time.Second)
	for stub.count() == 0 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	for _, id := range []string{"e1", "e2", "e3", "e4", "e5"} {
		rig.emit(id)
	}
	if !rig.errors.has(emission.ExportQueueFull) {
		t.Fatalf("no queue-full report: %v", rig.errors.codes)
	}
	close(release)
	rig.flush(t)
	stats := rig.tracer.Exporter.Stats()
	if stats.Dropped != 3 {
		t.Fatalf("dropped %d, want 3", stats.Dropped)
	}
	// Slots 1..3 exported; slots 4..6 were dropped, so they hold complete_through at 3.
	expectSlots(t, rig, 6, 3)
}

func TestRetryWithBackoffOnTransportErrorsThenSucceeds(t *testing.T) {
	stub := newStub(t, func(n int) stubReply {
		if n <= 2 {
			return stubReply{status: http.StatusServiceUnavailable, body: "restarting"}
		}
		return stubReply{}
	})
	rig := newRig(t, stub.URL, emission.AsyncOptions{MaxAttempts: 5, InitialBackoff: 100 * time.Millisecond, MaxBackoff: time.Second})
	rig.emit("a")
	rig.flush(t)
	expectSlots(t, rig, 1, 1)
	if stub.count() != 3 {
		t.Fatalf("attempts = %d, want 3", stub.count())
	}
	if len(*rig.sleeps) != 2 {
		t.Fatalf("backoffs = %v", *rig.sleeps)
	}
	first, second := (*rig.sleeps)[0], (*rig.sleeps)[1]
	if first < 80*time.Millisecond || first > 120*time.Millisecond || second < 160*time.Millisecond || second > 240*time.Millisecond {
		t.Fatalf("backoff did not double with bounded jitter: %v", *rig.sleeps)
	}
	if len(rig.errors.codes) != 0 {
		t.Fatalf("a recovered export must not be reported: %v", rig.errors.codes)
	}
}

func TestNoRetryOnNonTransportRefusalAndBodyIsSurfaced(t *testing.T) {
	stub := newStub(t, func(int) stubReply {
		return stubReply{status: http.StatusBadRequest, body: "unknown source handle shop-payment"}
	})
	rig := newRig(t, stub.URL, emission.AsyncOptions{MaxAttempts: 5})
	rig.emit("a")
	rig.flush(t)
	if stub.count() != 1 {
		t.Fatalf("a 400 was retried: %d attempts", stub.count())
	}
	expectSlots(t, rig, 1, 0)
	var found *emission.ExportError
	for _, e := range rig.errors.errs {
		if e.Code == emission.ExportRefused {
			found = e
		}
	}
	if found == nil || !strings.Contains(found.Error(), "unknown source handle shop-payment") {
		t.Fatalf("response body missing from the export error: %v", rig.errors.errs)
	}
}

func TestExhaustedSlotIsRetriedAndCatchesUp(t *testing.T) {
	var healthy atomic.Bool
	stub := newStub(t, func(int) stubReply {
		if !healthy.Load() {
			return stubReply{status: http.StatusBadGateway}
		}
		return stubReply{}
	})
	rig := newRig(t, stub.URL, emission.AsyncOptions{MaxAttempts: 2, RetryFailedInterval: 20 * time.Millisecond})
	rig.emit("a")
	rig.emit("b")
	rig.flush(t)
	expectSlots(t, rig, 2, 0)
	healthy.Store(true)
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if _, c := rig.declared(); c == 2 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	expectSlots(t, rig, 2, 2)
	if rig.tracer.Exporter.Stats().Retained != 0 {
		t.Fatal("retained spans were not released")
	}
}

func TestShutdownFlushesQueueAndRetriesRetainedOnce(t *testing.T) {
	var healthy atomic.Bool
	stub := newStub(t, func(int) stubReply {
		if !healthy.Load() {
			return stubReply{status: http.StatusServiceUnavailable}
		}
		return stubReply{}
	})
	rig := newRig(t, stub.URL, emission.AsyncOptions{MaxAttempts: 1, RetryFailedInterval: time.Hour})
	rig.emit("a")
	rig.flush(t)
	expectSlots(t, rig, 1, 0)
	healthy.Store(true)
	rig.emit("b")
	rig.emit("c")
	if err := rig.tracer.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
	expectSlots(t, rig, 3, 3)
	// A span started after Shutdown is a no-op in OTel's SDK: no slot, no panic, no block.
	rig.emit("late")
	expectSlots(t, rig, 3, 3)
	// The processor itself, ended directly after Shutdown, fails the slot instead of panicking.
	rig.tracer.Exporter.OnEnd(nil)
}

func TestShutdownHonoursItsContext(t *testing.T) {
	release := make(chan struct{})
	stub := newStub(t, func(int) stubReply { <-release; return stubReply{} })
	defer close(release)
	rig := newRig(t, stub.URL, emission.AsyncOptions{})
	rig.emit("a")
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	if err := rig.tracer.Shutdown(ctx); err == nil {
		t.Fatal("Shutdown past its deadline must say so")
	}
	expectSlots(t, rig, 1, 0)
}

func TestDuplicateEventIDKeepsTheSlotFailed(t *testing.T) {
	for _, kind := range []string{"evidence_conflict", "identity_conflict"} {
		t.Run(kind, func(t *testing.T) {
			stub := newStub(t, func(n int) stubReply {
				if n == 2 {
					return stubReply{rejected: 1, message: "append_events conflicted: " + kind}
				}
				return stubReply{}
			})
			rig := newRig(t, stub.URL, emission.AsyncOptions{BatchSize: 1})
			rig.emit("a")
			rig.flush(t)
			rig.emit("a") // an idempotent retry that re-runs the step under the same Event id: slot 2 is refused
			rig.emit("b")
			rig.flush(t)
			// Slots 1 and 3 exported; slot 2 FAILED, so complete_through stops at 1.
			expectSlots(t, rig, 3, 1)
			if !rig.errors.has(emission.ExportDuplicateEventID) {
				t.Fatalf("no duplicate-event-id report: %v", rig.errors.codes)
			}
			if rig.tracer.Exporter.Stats().Duplicates != 1 {
				t.Fatalf("stats %+v", rig.tracer.Exporter.Stats())
			}
			// Not retried: three spans, three requests, even after a flush that would retry retained spans.
			rig.flush(t)
			if got := stub.count(); got != 3 {
				t.Fatalf("requests = %d, want 3 (a duplicate refusal must not start a retry loop)", got)
			}
		})
	}
}

func TestOtherRejectionsKeepTheSlotFailed(t *testing.T) {
	stub := newStub(t, func(int) stubReply {
		return stubReply{rejected: 1, message: "append_events conflicted: slot_conflict"}
	})
	rig := newRig(t, stub.URL, emission.AsyncOptions{})
	rig.emit("a")
	rig.flush(t)
	expectSlots(t, rig, 1, 0)
	if !rig.errors.has(emission.ExportRejected) || rig.errors.has(emission.ExportDuplicateEventID) {
		t.Fatalf("codes %v", rig.errors.codes)
	}
}

// One span that breaks the contract must not fail the spans batched with it.
func TestBadSpanInABatchIsIsolated(t *testing.T) {
	release := make(chan struct{})
	stub := newStub(t, func(n int) stubReply {
		if n == 1 {
			<-release
		}
		return stubReply{}
	})
	rig := newRig(t, stub.URL, emission.AsyncOptions{BatchSize: 8})
	rig.emit("a")
	deadline := time.Now().Add(2 * time.Second)
	for stub.count() == 0 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	// Slot 2 carries a sentinel.sequence pair the stamper will not touch but no valid claimed sensitivity.
	rig.emit("bad", attribute.String(emission.AttributeClaimedSensitivity, "SENSITIVITY_NOPE"))
	rig.emit("c")
	close(release)
	rig.flush(t)
	expectSlots(t, rig, 3, 1)
	allocated := rig.allocator
	allocated.MarkExported(emission.Slot{Epoch: allocated.Epoch(), Number: 2})
	expectSlots(t, rig, 3, 3)
	if !rig.errors.has(emission.ExportEncode) {
		t.Fatalf("codes %v", rig.errors.codes)
	}
}

func TestPanickingOnErrorDoesNotEscape(t *testing.T) {
	stub := newStub(t, func(int) stubReply { return stubReply{status: http.StatusBadRequest} })
	allocator, _ := emission.NewSequenceAllocator(emission.SequenceOptions{})
	tracer := emission.NewProbeTracer(emission.Options{
		TracerName: "probe.test", Endpoint: stub.URL, SourceHandle: "shop-payment", Sequence: allocator,
		Async:   emission.AsyncOptions{Synchronous: true},
		OnError: func(error) { panic("host callback bug") },
		Logf:    func(string, ...any) {},
		HostStamp: &emission.HostStamperOptions{
			SourceHandle: "shop-payment", SchemaVersion: "v1",
		},
	})
	defer func() { _ = tracer.Shutdown(context.Background()) }()
	_, span := tracer.Tracer.Start(context.Background(), "x")
	span.End() // must not panic
	if d := allocator.Declaration(); d.CompleteThrough != 0 || d.LastEmitted != 1 {
		t.Fatalf("declaration %+v", d)
	}
}

// A remote parent that is not a Probe exports a ROOT span: the Sentinel copy has no parent span id at all.
func TestUnattributedRemoteParentExportsARootSpanThroughTheTracer(t *testing.T) {
	stub := newStub(t, func(int) stubReply { return stubReply{} })
	allocator, _ := emission.NewSequenceAllocator(emission.SequenceOptions{})
	tracer := emission.NewProbeTracer(emission.Options{
		TracerName: "probe.test", Endpoint: stub.URL, SourceHandle: "shop-payment", Sequence: allocator,
		Export: otlp.ExportOptions{AllowUnattributedRemoteParent: true},
		Logf:   func(string, ...any) {},
		HostStamp: &emission.HostStamperOptions{
			SourceHandle: "shop-payment", SchemaVersion: "v1",
		},
	})
	remote := trace.NewSpanContext(trace.SpanContextConfig{
		TraceID: trace.TraceID{1}, SpanID: trace.SpanID{2}, TraceFlags: trace.FlagsSampled, Remote: true,
	})
	ctx := trace.ContextWithRemoteSpanContext(context.Background(), remote)
	_, span := tracer.Tracer.Start(ctx, "shop.payment.charge")
	span.End()
	if err := tracer.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
	if d := allocator.Declaration(); d.CompleteThrough != 1 {
		t.Fatalf("declaration %+v", d)
	}
	stub.mu.Lock()
	defer stub.mu.Unlock()
	wire := stub.spans[0].GetResourceSpans()[0].GetScopeSpans()[0].GetSpans()[0]
	if len(wire.GetParentSpanId()) != 0 {
		t.Fatalf("parent span id %x on the Sentinel copy of a root span", wire.GetParentSpanId())
	}
	for _, kv := range wire.GetAttributes() {
		if kv.GetKey() == "sentinel.parent.event.id" {
			t.Fatalf("an unattributed remote parent must carry no parent Event id")
		}
	}
}

func TestHostStamperSetsReservedAttributesAndKeepsHostValues(t *testing.T) {
	stub := newStub(t, func(int) stubReply { return stubReply{} })
	allocator, _ := emission.NewSequenceAllocator(emission.SequenceOptions{})
	var epoch uint64 = 7
	tracer := emission.NewProbeTracer(emission.Options{
		TracerName: "probe.test", Endpoint: stub.URL, SourceHandle: "shop-payment", Sequence: allocator,
		Logf: func(string, ...any) {},
		HostStamp: &emission.HostStamperOptions{
			SourceHandle: "shop-payment", SchemaVersion: "sagashop-contract-v1",
			ClaimedSensitivity: "SENSITIVITY_CONFIDENTIAL", FilterEpoch: func() *uint64 { return &epoch },
		},
	})
	ctx, parent := tracer.Tracer.Start(context.Background(), "parent")
	_, child := tracer.Tracer.Start(ctx, "child")
	child.End()
	parent.End()
	_, own := tracer.Tracer.Start(context.Background(), "own", trace.WithAttributes(
		attribute.String(emission.AttributeEventID, "payment.charged:o-1"),
		attribute.String(emission.AttributeClaimedSensitivity, "SENSITIVITY_RESTRICTED")))
	own.End()
	if err := tracer.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
	if d := allocator.Declaration(); d.CompleteThrough != 3 {
		t.Fatalf("declaration %+v", d)
	}
	byName := map[string]map[string]*attrValue{}
	stub.mu.Lock()
	defer stub.mu.Unlock()
	for _, request := range stub.spans {
		for _, rs := range request.GetResourceSpans() {
			for _, span := range rs.GetScopeSpans()[0].GetSpans() {
				attrs := map[string]*attrValue{}
				for _, kv := range span.GetAttributes() {
					attrs[kv.GetKey()] = &attrValue{s: kv.GetValue().GetStringValue(), i: kv.GetValue().GetIntValue()}
				}
				byName[span.GetName()] = attrs
			}
		}
	}
	childAttrs, parentAttrs := byName["child"], byName["parent"]
	if childAttrs["sentinel.parent.event.id"].s != parentAttrs["sentinel.event.id"].s || parentAttrs["sentinel.event.id"].s == "" {
		t.Fatalf("child does not name its parent's Event id: %+v %+v", childAttrs, parentAttrs)
	}
	if !strings.HasPrefix(parentAttrs["sentinel.event.id"].s, "shop-payment:") ||
		parentAttrs["sentinel.schema.version"].s != "sagashop-contract-v1" ||
		parentAttrs["sentinel.acknowledged_filter_epoch"].i != 7 ||
		parentAttrs["sentinel.claimed_sensitivity"].s != "SENSITIVITY_CONFIDENTIAL" {
		t.Fatalf("parent attributes %+v", parentAttrs)
	}
	ownAttrs := byName["own"]
	if ownAttrs["sentinel.event.id"].s != "payment.charged:o-1" || ownAttrs["sentinel.claimed_sensitivity"].s != "SENSITIVITY_RESTRICTED" {
		t.Fatalf("host-set attributes were overwritten: %+v", ownAttrs)
	}
}

type attrValue struct {
	s string
	i int64
}

func TestExportErrorsAreLoggedAtMostOncePerInterval(t *testing.T) {
	stub := newStub(t, func(int) stubReply { return stubReply{status: http.StatusBadRequest} })
	allocator, _ := emission.NewSequenceAllocator(emission.SequenceOptions{})
	var mu sync.Mutex
	var lines []string
	tracer := emission.NewProbeTracer(emission.Options{
		TracerName: "probe.test", Endpoint: stub.URL, SourceHandle: "shop-payment", Sequence: allocator,
		Async: emission.AsyncOptions{Synchronous: true}, ErrorLogInterval: time.Hour,
		Logf: func(format string, args ...any) {
			mu.Lock()
			defer mu.Unlock()
			lines = append(lines, format)
		},
		HostStamp: &emission.HostStamperOptions{SourceHandle: "shop-payment", SchemaVersion: "v1"},
	})
	defer func() { _ = tracer.Shutdown(context.Background()) }()
	for i := 0; i < 10; i++ {
		_, span := tracer.Tracer.Start(context.Background(), "x")
		span.End()
	}
	mu.Lock()
	defer mu.Unlock()
	if len(lines) != 1 {
		t.Fatalf("%d log lines for 10 failures", len(lines))
	}
}

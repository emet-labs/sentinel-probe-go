package emission_test

import (
	"context"
	"encoding/hex"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/emet-labs/sentinel/sdk/go/emission"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"
)

type sentinelReply struct {
	status int
	body   string
}

// fakeSentinel answers each trace export on 127.0.0.1 with the next reply.
func fakeSentinel(t *testing.T, replies ...sentinelReply) (string, func() int) {
	t.Helper()
	var mu sync.Mutex
	received := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		mu.Lock()
		reply := sentinelReply{status: http.StatusOK}
		if received < len(replies) {
			reply = replies[received]
		}
		received++
		mu.Unlock()
		body, _ := hex.DecodeString(reply.body)
		w.Header().Set("Content-Type", "application/x-protobuf")
		w.WriteHeader(reply.status)
		_, _ = w.Write(body)
	}))
	t.Cleanup(server.Close)
	return server.URL, func() int {
		mu.Lock()
		defer mu.Unlock()
		return received
	}
}

// hostAttributes are the #398 attributes a host sets, without the sequence pair.
func hostAttributes(eventID string) []attribute.KeyValue {
	return []attribute.KeyValue{
		attribute.String(emission.AttributeEventID, eventID),
		attribute.String(emission.AttributeSchemaVersion, "sagashop-contract-v1"),
		attribute.Int64(emission.AttributeAcknowledgedFilterEpoch, 0),
		attribute.String(emission.AttributeClaimedSensitivity, "SENSITIVITY_INTERNAL"),
	}
}

func sequencedTracer(t *testing.T, endpoint string, allocator *emission.SequenceAllocator, processors ...sdktrace.SpanProcessor) *emission.ProbeTracer {
	t.Helper()
	options := make([]sdktrace.TracerProviderOption, 0, len(processors))
	for _, processor := range processors {
		options = append(options, sdktrace.WithSpanProcessor(processor))
	}
	tracer := emission.NewProbeTracer(emission.Options{
		TracerName:   "probe.test",
		Endpoint:     endpoint,
		SourceHandle: "sagashop-payment",
		Sequence:     allocator,
		// Host processors are registered before the stamper and the exporter.
		ProviderOptions: options,
	})
	t.Cleanup(func() { _ = tracer.Provider.Shutdown(context.Background()) })
	return tracer
}

func emitCharge(tracer *emission.ProbeTracer, eventID string, extra ...attribute.KeyValue) {
	start := time.Unix(1_700_000_000, 0).UTC()
	_, span := tracer.Tracer.Start(context.Background(), "sagashop.payment.charge.completed",
		trace.WithTimestamp(start),
		trace.WithAttributes(append(hostAttributes(eventID), extra...)...))
	span.End(trace.WithTimestamp(start.Add(time.Millisecond)))
}

// silenceOTel keeps expected export errors out of the test log.
func silenceOTel(t *testing.T) {
	t.Helper()
	previous := otel.GetErrorHandler()
	otel.SetErrorHandler(otel.ErrorHandlerFunc(func(error) {}))
	t.Cleanup(func() { otel.SetErrorHandler(previous) })
}

func TestBridgeStampsAtStartAndMarksExported(t *testing.T) {
	endpoint, received := fakeSentinel(t)
	allocator := newAllocator(t, &manualClock{ms: int64(epochE)}, nil, nil)
	var stamped []attribute.KeyValue
	end := &endRecorder{onEnd: func(span sdktrace.ReadOnlySpan) { stamped = span.Attributes() }}
	tracer := sequencedTracer(t, endpoint, allocator, end)
	emitCharge(tracer, "charge-1")
	if received() != 1 {
		t.Fatalf("received %d exports", received())
	}
	if got := attributeInt(stamped, emission.AttributeSequenceEpoch); got != int64(epochE) {
		t.Fatalf("stamped epoch %d", got)
	}
	if got := attributeInt(stamped, emission.AttributeSequenceNumber); got != 1 {
		t.Fatalf("stamped number %d", got)
	}
	expectDeclared(t, allocator, 1, 1)
}

func TestBridgeStampsBeforeEnd(t *testing.T) {
	endpoint, _ := fakeSentinel(t)
	allocator := newAllocator(t, &manualClock{ms: int64(epochE)}, nil, nil)
	tracer := sequencedTracer(t, endpoint, allocator)
	start := time.Unix(1_700_000_000, 0).UTC()
	_, span := tracer.Tracer.Start(context.Background(), "sagashop.payment.charge.completed",
		trace.WithTimestamp(start), trace.WithAttributes(hostAttributes("charge-1")...))
	readable, ok := span.(sdktrace.ReadOnlySpan)
	if !ok {
		t.Fatal("SDK span is not readable")
	}
	if attributeInt(readable.Attributes(), emission.AttributeSequenceNumber) != 1 {
		t.Fatalf("not stamped at start: %v", readable.Attributes())
	}
	expectDeclared(t, allocator, 1, 0)
	span.End(trace.WithTimestamp(start.Add(time.Millisecond)))
	expectDeclared(t, allocator, 1, 1)
}

func TestBridgeMarksFailedOnRejectionErrorOrUndecodableBody(t *testing.T) {
	silenceOTel(t)
	endpoint, received := fakeSentinel(t,
		sentinelReply{http.StatusOK, "0a020801"},
		sentinelReply{http.StatusInternalServerError, ""},
		sentinelReply{http.StatusOK, "0a0508"},
		sentinelReply{http.StatusOK, ""},
	)
	allocator := newAllocator(t, &manualClock{ms: int64(epochE)}, nil, nil)
	tracer := sequencedTracer(t, endpoint, allocator)
	for _, id := range []string{"charge-1", "charge-2", "charge-3", "charge-4"} {
		emitCharge(tracer, id)
	}
	if received() != 4 {
		t.Fatalf("received %d", received())
	}
	expectDeclared(t, allocator, 4, 0)
	// FAILED, not lost: host re-exports that succeed release the prefix.
	for number := uint64(1); number <= 3; number++ {
		allocator.MarkExported(emission.Slot{Epoch: epochE, Number: number})
	}
	expectDeclared(t, allocator, 4, 4)
}

func TestBridgeLeavesAHostReservedSlotAlone(t *testing.T) {
	endpoint, _ := fakeSentinel(t)
	allocator := newAllocator(t, &manualClock{ms: int64(epochE)}, nil, nil)
	reserved := allocate(t, allocator)
	tracer := sequencedTracer(t, endpoint, allocator)
	emitCharge(tracer, "charge-1",
		attribute.Int64(emission.AttributeSequenceEpoch, int64(reserved.Epoch)),
		attribute.Int64(emission.AttributeSequenceNumber, int64(reserved.Number)))
	if allocator.HighestAllocated() != 1 {
		t.Fatalf("re-allocated: highest %d", allocator.HighestAllocated())
	}
	expectDeclared(t, allocator, 1, 1)
}

func TestBridgeMarksAClientSideRefusalFailed(t *testing.T) {
	silenceOTel(t)
	endpoint, received := fakeSentinel(t)
	allocator := newAllocator(t, &manualClock{ms: int64(epochE)}, nil, nil)
	tracer := sequencedTracer(t, endpoint, allocator)
	start := time.Unix(1_700_000_000, 0).UTC()
	// No sentinel.event.id: BuildExport refuses the span and posts nothing.
	_, span := tracer.Tracer.Start(context.Background(), "sagashop.payment.charge.completed",
		trace.WithTimestamp(start), trace.WithAttributes(hostAttributes("charge-1")[1:]...))
	span.End(trace.WithTimestamp(start.Add(time.Millisecond)))
	if received() != 0 {
		t.Fatalf("posted %d", received())
	}
	expectDeclared(t, allocator, 1, 0)
	allocator.MarkExported(emission.Slot{Epoch: epochE, Number: 1})
	expectDeclared(t, allocator, 1, 1)
}

func TestSequenceWithoutEndpointPanics(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Fatal("no panic")
		}
	}()
	emission.NewProbeTracer(emission.Options{TracerName: "probe.test", Sequence: newAllocator(t, &manualClock{}, nil, nil)})
}

type endRecorder struct {
	onEnd func(sdktrace.ReadOnlySpan)
}

func (*endRecorder) OnStart(context.Context, sdktrace.ReadWriteSpan) {}
func (r *endRecorder) OnEnd(span sdktrace.ReadOnlySpan)              { r.onEnd(span) }
func (*endRecorder) Shutdown(context.Context) error                  { return nil }
func (*endRecorder) ForceFlush(context.Context) error                { return nil }

func attributeInt(attributes []attribute.KeyValue, key string) int64 {
	for _, kv := range attributes {
		if string(kv.Key) == key && kv.Value.Type() == attribute.INT64 {
			return kv.Value.AsInt64()
		}
	}
	return -1
}

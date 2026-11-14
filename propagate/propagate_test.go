package propagate_test

import (
	"context"
	"net/http"
	"testing"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"

	"github.com/emet-labs/sentinel/sdk/go/propagate"
)

func TestInjectExtractRoundTripOnHTTPHeaders(t *testing.T) {
	header := http.Header{}
	propagate.InjectParentEventID(header, "payment.charged:o-1")
	if got := header.Get("x-sentinel-parent-event-id"); got != "payment.charged:o-1" {
		t.Fatalf("header = %q", got)
	}
	if got := header.Get("tracestate"); got != "sentinel=payment.charged:o-1" {
		t.Fatalf("tracestate = %q", got)
	}
	if got := propagate.ExtractParentEventID(header); got != "payment.charged:o-1" {
		t.Fatalf("extract = %q", got)
	}
}

func TestExtractFallsBackToTraceStateAndPrefersTheHeader(t *testing.T) {
	onlyState := http.Header{}
	onlyState.Set("tracestate", "vendor=1,sentinel=ship.requested:9")
	if got := propagate.ExtractParentEventID(onlyState); got != "ship.requested:9" {
		t.Fatalf("fallback = %q", got)
	}
	both := http.Header{}
	both.Set("tracestate", "sentinel=from-state")
	both.Set("x-sentinel-parent-event-id", "from-header")
	if got := propagate.ExtractParentEventID(both); got != "from-header" {
		t.Fatalf("precedence = %q", got)
	}
}

func TestInjectMergesExistingTraceStateAndReplacesAnOlderMember(t *testing.T) {
	header := http.Header{}
	header.Set("tracestate", "vendor=1,sentinel=old")
	propagate.InjectParentEventID(header, "new")
	if got := header.Get("tracestate"); got != "sentinel=new,vendor=1" {
		t.Fatalf("tracestate = %q", got)
	}
}

func TestNonProbeCallerAndMalformedInputYieldNoID(t *testing.T) {
	if got := propagate.ExtractParentEventID(http.Header{}); got != "" {
		t.Fatalf("empty carrier: %q", got)
	}
	bad := http.Header{}
	bad.Set("tracestate", "###not valid###")
	if got := propagate.ExtractParentEventID(bad); got != "" {
		t.Fatalf("malformed tracestate: %q", got)
	}
	untouched := http.Header{}
	propagate.InjectParentEventID(untouched, "")
	propagate.InjectParentEventID(nil, "x")
	if len(untouched) != 0 {
		t.Fatalf("an empty id wrote %v", untouched)
	}
	// A malformed upstream tracestate still gets the header.
	propagate.InjectParentEventID(bad, "id")
	if bad.Get("x-sentinel-parent-event-id") != "id" {
		t.Fatal("header not written")
	}
}

func TestMapCarrierIsCaseInsensitive(t *testing.T) {
	carrier := propagate.MapCarrier{}
	propagate.InjectParentEventID(carrier, "e-1")
	if carrier.Get("X-Sentinel-Parent-Event-Id") != "e-1" || propagate.ExtractParentEventID(carrier) != "e-1" {
		t.Fatalf("%v", carrier)
	}
}

func TestPropagatorCarriesTheCurrentSpansEventIDAndKeepsTheCallers(t *testing.T) {
	provider := sdktrace.NewTracerProvider()
	ctx, span := provider.Tracer("t").Start(context.Background(), "s",
		trace.WithAttributes(attribute.String("sentinel.event.id", "payment.charged:o-1")))
	defer span.End()
	p := propagate.Propagator{Inner: propagation.TraceContext{}}

	carrier := propagation.HeaderCarrier(http.Header{})
	p.Inject(ctx, carrier)
	if carrier.Get("x-sentinel-parent-event-id") != "payment.charged:o-1" || carrier.Get("traceparent") == "" {
		t.Fatalf("inject wrote %v", http.Header(carrier))
	}
	if len(p.Fields()) != 3 {
		t.Fatalf("fields %v", p.Fields())
	}

	received := p.Extract(context.Background(), carrier)
	if propagate.CallerEventID(received) != "payment.charged:o-1" {
		t.Fatalf("caller event id %q", propagate.CallerEventID(received))
	}
	if got := propagate.ParentEventIDFor(received); got != "payment.charged:o-1" {
		t.Fatalf("parent event id for a remote parent = %q", got)
	}
	// A caller that is not a Probe: a remote parent with no Event id yields no edge.
	foreign := propagation.HeaderCarrier(http.Header{})
	foreign.Set("traceparent", "00-0102030405060708090a0b0c0d0e0f10-1112131415161718-01")
	if got := propagate.ParentEventIDFor(p.Extract(context.Background(), foreign)); got != "" {
		t.Fatalf("foreign caller produced edge %q", got)
	}
	// A local parent yields its own Event id.
	if got := propagate.ParentEventIDFor(ctx); got != "payment.charged:o-1" {
		t.Fatalf("local parent = %q", got)
	}
}

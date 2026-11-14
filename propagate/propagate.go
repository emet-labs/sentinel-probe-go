// Package propagate carries a Probe's current Event id across a process boundary, so the Event the callee
// exports names the caller's Event as its causal parent (sentinel.parent.event.id).
//
// The convention, shared by every Sentinel Probe SDK:
//
//   - the sender writes the header "x-sentinel-parent-event-id: <eventId>", and also a `sentinel=<eventId>`
//     member of the W3C tracestate header, which rides along with traceparent through an OTel-aware
//     intermediary that drops unknown headers;
//   - the receiver prefers the header and falls back to the tracestate member;
//   - a caller that is not a Probe sends neither, and the receiver exports its Event as a causal root.
//
// A Carrier is the two methods http.Header already has, so http.Header, a Kafka header adapter and an OTel
// propagation.TextMapCarrier all fit. MapCarrier is a ready map for tests and simple transports.
package propagate

import (
	"context"
	"strings"

	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"

	"github.com/emet-labs/sentinel/sdk/go/otlp"
)

const (
	// ParentEventIDHeader carries the caller's Event id.
	ParentEventIDHeader = "x-sentinel-parent-event-id"
	// TraceStateHeader is the W3C tracestate header.
	TraceStateHeader = "tracestate"
	// TraceStateKey is the tracestate member that carries the caller's Event id.
	TraceStateKey = "sentinel"
)

// Carrier is a header set the Event id is written to and read from. http.Header satisfies it. A Kafka client's
// headers are a slice, not a map: wrap them in a type with these two methods (Get returns the last or first
// value of the key, "" when absent; Set replaces it).
type Carrier interface {
	Get(key string) string
	Set(key, value string)
}

// MapCarrier is a Carrier over a map with case-insensitive keys, for tests and simple transports.
type MapCarrier map[string]string

// Get returns the value for key, or "".
func (m MapCarrier) Get(key string) string { return m[strings.ToLower(key)] }

// Set stores value under key.
func (m MapCarrier) Set(key, value string) { m[strings.ToLower(key)] = value }

// Keys lists the stored keys, so a MapCarrier also satisfies propagation.TextMapCarrier.
func (m MapCarrier) Keys() []string {
	keys := make([]string, 0, len(m))
	for key := range m {
		keys = append(keys, key)
	}
	return keys
}

// InjectParentEventID writes eventID to the carrier as the header and as a tracestate member, merging with any
// tracestate the carrier already holds (the sentinel member is placed first, replacing an older one). An empty
// eventID writes nothing. A value tracestate cannot carry still goes in the header.
func InjectParentEventID(carrier Carrier, eventID string) {
	if carrier == nil || eventID == "" {
		return
	}
	carrier.Set(ParentEventIDHeader, eventID)
	state, err := trace.ParseTraceState(carrier.Get(TraceStateHeader))
	if err != nil {
		// A malformed upstream tracestate is not ours to repair; the header already carries the id.
		return
	}
	if updated, err := state.Insert(TraceStateKey, eventID); err == nil {
		carrier.Set(TraceStateHeader, updated.String())
	}
}

// ExtractParentEventID returns the Event id the sender wrote: the header if present, else the tracestate
// member. It returns "" when the caller is not a Probe. It never errors: a malformed tracestate yields "".
func ExtractParentEventID(carrier Carrier) string {
	if carrier == nil {
		return ""
	}
	if id := strings.TrimSpace(carrier.Get(ParentEventIDHeader)); id != "" {
		return id
	}
	return eventIDFromTraceState(carrier.Get(TraceStateHeader))
}

// ParentEventIDFromHeaders is ExtractParentEventID for a handler that holds the two header values already.
func ParentEventIDFromHeaders(parentEventIDHeader, tracestate string) string {
	if id := strings.TrimSpace(parentEventIDHeader); id != "" {
		return id
	}
	return eventIDFromTraceState(tracestate)
}

func eventIDFromTraceState(tracestate string) string {
	state, err := trace.ParseTraceState(tracestate)
	if err != nil {
		return ""
	}
	return state.Get(TraceStateKey)
}

type callerEventKey struct{}

// WithCallerEventID returns ctx carrying the Event id of a remote caller; HostStamper reads it for the next
// span's sentinel.parent.event.id when that span's parent is remote.
func WithCallerEventID(ctx context.Context, eventID string) context.Context {
	if eventID == "" {
		return ctx
	}
	return context.WithValue(ctx, callerEventKey{}, eventID)
}

// CallerEventID returns the remote caller's Event id stored by WithCallerEventID or Propagator.Extract.
func CallerEventID(ctx context.Context) string {
	id, _ := ctx.Value(callerEventKey{}).(string)
	return id
}

// Propagator wraps an OTel TextMapPropagator (normally propagation.TraceContext{}). On Inject it writes the
// current span's Event id with InjectParentEventID, so every instrumented HTTP client, gRPC client and message
// producer carries it; on Extract it keeps the sender's Event id in the context (CallerEventID). Install it with
// otel.SetTextMapPropagator.
type Propagator struct{ Inner propagation.TextMapPropagator }

var _ propagation.TextMapPropagator = Propagator{}

type textMapAdapter struct{ propagation.TextMapCarrier }

func (a textMapAdapter) Set(key, value string) { a.TextMapCarrier.Set(key, value) }

// Inject implements propagation.TextMapPropagator.
func (p Propagator) Inject(ctx context.Context, carrier propagation.TextMapCarrier) {
	if id := EventIDOf(trace.SpanFromContext(ctx)); id != "" {
		InjectParentEventID(textMapAdapter{carrier}, id)
	}
	p.inner().Inject(ctx, carrier)
}

// Extract implements propagation.TextMapPropagator.
func (p Propagator) Extract(ctx context.Context, carrier propagation.TextMapCarrier) context.Context {
	ctx = p.inner().Extract(ctx, carrier)
	return WithCallerEventID(ctx, ExtractParentEventID(textMapAdapter{carrier}))
}

// Fields implements propagation.TextMapPropagator.
func (p Propagator) Fields() []string {
	return append(append([]string{}, p.inner().Fields()...), ParentEventIDHeader)
}

func (p Propagator) inner() propagation.TextMapPropagator {
	if p.Inner == nil {
		return propagation.TraceContext{}
	}
	return p.Inner
}

// EventIDOf is the sentinel.event.id attribute of a started SDK span, or "".
func EventIDOf(span trace.Span) string {
	readable, ok := span.(sdktrace.ReadOnlySpan)
	if !ok {
		return ""
	}
	for _, kv := range readable.Attributes() {
		if string(kv.Key) == otlp.AttributeEventID {
			return kv.Value.AsString()
		}
	}
	return ""
}

// ParentEventIDFor is the Event id a span started with parent should name as sentinel.parent.event.id: the
// parent span's own Event id in this process, or the id a remote caller sent (CallerEventID, else the
// tracestate member of the remote span context). "" means no edge.
func ParentEventIDFor(parent context.Context) string {
	spanContext := trace.SpanContextFromContext(parent)
	if !spanContext.IsValid() {
		return ""
	}
	if spanContext.IsRemote() {
		if id := CallerEventID(parent); id != "" {
			return id
		}
		return spanContext.TraceState().Get(TraceStateKey)
	}
	return EventIDOf(trace.SpanFromContext(parent))
}

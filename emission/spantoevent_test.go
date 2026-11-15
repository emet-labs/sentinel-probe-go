package emission_test

import (
	"context"
	"slices"
	"testing"
	"time"

	"go.opentelemetry.io/otel/attribute"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"

	"github.com/emet-labs/sentinel-probe-go/emission"
	modelv1 "github.com/emet-labs/sentinel-probe-go/gen/sentinel/model/v1"
)

// sdktrace.ReadOnlySpan is a sealed interface (it declares an unexported private() method), so
// tracetest.SpanStub{...}.Snapshot() is the only way to construct one outside the OTel SDK.
// Every span in this file goes through it.

var startTime = time.Unix(1700000000, 123456789).UTC()

func spanContext(t *testing.T, traceHex, spanHex string) trace.SpanContext {
	t.Helper()
	traceID, err := trace.TraceIDFromHex(traceHex)
	if err != nil {
		t.Fatalf("trace id: %v", err)
	}
	spanID, err := trace.SpanIDFromHex(spanHex)
	if err != nil {
		t.Fatalf("span id: %v", err)
	}
	return trace.NewSpanContext(trace.SpanContextConfig{TraceID: traceID, SpanID: spanID})
}

func snapshot(stub tracetest.SpanStub) sdktrace.ReadOnlySpan {
	if stub.StartTime.IsZero() {
		stub.StartTime = startTime
	}
	return stub.Snapshot()
}

func convert(span sdktrace.ReadOnlySpan) *modelv1.ProducerEvent {
	return emission.SpanToEvent(emission.SpanConversion{
		Span:          span,
		SchemaVersion: "sentinel.model.v1",
		EventID:       "evt-1",
	})
}

func attributeValue(t *testing.T, event *modelv1.ProducerEvent, key string) *modelv1.AttributeValue {
	t.Helper()
	for _, entry := range event.GetAttributes() {
		if entry.GetKey() == key {
			return entry.GetValue()
		}
	}
	t.Fatalf("attribute %q not emitted; got %v", key, event.GetAttributes())
	return nil
}

func TestSpanNameBecomesEventKind(t *testing.T) {
	t.Parallel()

	event := convert(snapshot(tracetest.SpanStub{Name: "transfer.initiated"}))
	if event.GetKind() != "transfer.initiated" {
		t.Fatalf("Kind = %q", event.GetKind())
	}
}

func TestEventIDAndSchemaVersionArePassedThrough(t *testing.T) {
	t.Parallel()

	event := emission.SpanToEvent(emission.SpanConversion{
		Span:          snapshot(tracetest.SpanStub{Name: "x"}),
		SchemaVersion: "sentinel.model.v1",
		EventID:       "evt-42",
	})
	if event.GetId() != "evt-42" {
		t.Errorf("Id = %q", event.GetId())
	}
	if event.GetSchemaVersion() != "sentinel.model.v1" {
		t.Errorf("SchemaVersion = %q", event.GetSchemaVersion())
	}
}

func TestAcknowledgedEpochIsStampedIncludingZero(t *testing.T) {
	t.Parallel()

	for _, epoch := range []uint64{0, 42} {
		event := emission.SpanToEvent(emission.SpanConversion{
			Span:              snapshot(tracetest.SpanStub{Name: "x"}),
			AcknowledgedEpoch: &epoch,
		})
		if event.AcknowledgedFilterEpoch == nil {
			t.Fatalf("epoch %d must be stamped as present", epoch)
		}
		if *event.AcknowledgedFilterEpoch != epoch {
			t.Fatalf("AcknowledgedFilterEpoch = %d, want %d", *event.AcknowledgedFilterEpoch, epoch)
		}
	}

	event := emission.SpanToEvent(emission.SpanConversion{Span: snapshot(tracetest.SpanStub{Name: "x"})})
	if event.AcknowledgedFilterEpoch != nil {
		t.Fatal("an absent epoch must stay absent, not become 0")
	}
}

func TestSequenceCapabilitiesAndSensitivityArePassedThrough(t *testing.T) {
	t.Parallel()

	event := emission.SpanToEvent(emission.SpanConversion{
		Span:     snapshot(tracetest.SpanStub{Name: "x"}),
		Sequence: &modelv1.SequenceCoordinate{Epoch: 2, Sequence: 11},
		ClaimedCapabilities: []modelv1.SourceCapability{
			modelv1.SourceCapability_SOURCE_CAPABILITY_CAUSAL_EDGES,
			modelv1.SourceCapability_SOURCE_CAPABILITY_OBSERVE_BEFORE_EFFECT,
		},
		ClaimedSensitivity: modelv1.Sensitivity_SENSITIVITY_RESTRICTED,
	})
	if event.GetSequence().GetEpoch() != 2 || event.GetSequence().GetSequence() != 11 {
		t.Errorf("Sequence = %v", event.GetSequence())
	}
	if len(event.GetClaimedCapabilities()) != 2 {
		t.Errorf("ClaimedCapabilities = %v", event.GetClaimedCapabilities())
	}
	if event.GetClaimedSensitivity() != modelv1.Sensitivity_SENSITIVITY_RESTRICTED {
		t.Errorf("ClaimedSensitivity = %v", event.GetClaimedSensitivity())
	}
}

func TestStringAttributeMapsToStringValue(t *testing.T) {
	t.Parallel()

	event := convert(snapshot(tracetest.SpanStub{
		Name:       "x",
		Attributes: []attribute.KeyValue{attribute.String("sagashop.order.id", "ord-9")},
	}))
	if got := attributeValue(t, event, "sagashop.order.id").GetStringValue(); got != "ord-9" {
		t.Fatalf("string_value = %q", got)
	}
}

func TestBoolAttributeMapsToBoolValue(t *testing.T) {
	t.Parallel()

	event := convert(snapshot(tracetest.SpanStub{
		Name:       "x",
		Attributes: []attribute.KeyValue{attribute.Bool("approved", true)},
	}))
	value := attributeValue(t, event, "approved")
	if _, ok := value.GetValue().(*modelv1.AttributeValue_BoolValue); !ok {
		t.Fatalf("expected bool_value, got %T", value.GetValue())
	}
	if !value.GetBoolValue() {
		t.Fatal("bool_value = false")
	}
}

func TestInt64AttributeMapsToIntegerValue(t *testing.T) {
	t.Parallel()

	event := convert(snapshot(tracetest.SpanStub{
		Name:       "x",
		Attributes: []attribute.KeyValue{attribute.Int64("sagashop.amount_cents", -4200)},
	}))
	value := attributeValue(t, event, "sagashop.amount_cents")
	if _, ok := value.GetValue().(*modelv1.AttributeValue_IntegerValue); !ok {
		t.Fatalf("expected integer_value, got %T", value.GetValue())
	}
	if value.GetIntegerValue() != -4200 {
		t.Fatalf("integer_value = %d", value.GetIntegerValue())
	}
}

// TestFloat64AttributeMapsToDoubleValueEvenWhenIntegral pins divergence D14. TypeScript
// branches on Number.isInteger, so JavaScript 3.0 becomes integer_value; Go dispatches on the
// attribute type, so float64(3) stays a double. Go's behaviour is the correct one.
func TestFloat64AttributeMapsToDoubleValueEvenWhenIntegral(t *testing.T) {
	t.Parallel()

	event := convert(snapshot(tracetest.SpanStub{
		Name: "x",
		Attributes: []attribute.KeyValue{
			attribute.Float64("ratio", 0.25),
			attribute.Float64("integral", 3),
		},
	}))
	if got := attributeValue(t, event, "ratio").GetDoubleValue(); got != 0.25 {
		t.Fatalf("double_value = %v", got)
	}
	integral := attributeValue(t, event, "integral")
	if _, ok := integral.GetValue().(*modelv1.AttributeValue_DoubleValue); !ok {
		t.Fatalf("float64(3) must stay a double, got %T", integral.GetValue())
	}
}

func TestByteSliceAttributeMapsToBytesValue(t *testing.T) {
	t.Parallel()

	event := convert(snapshot(tracetest.SpanStub{
		Name:       "x",
		Attributes: []attribute.KeyValue{attribute.ByteSlice("payload", []byte{0xde, 0xad, 0xbe, 0xef})},
	}))
	value := attributeValue(t, event, "payload")
	if _, ok := value.GetValue().(*modelv1.AttributeValue_BytesValue); !ok {
		t.Fatalf("expected bytes_value, got %T", value.GetValue())
	}
	if !slices.Equal(value.GetBytesValue(), []byte{0xde, 0xad, 0xbe, 0xef}) {
		t.Fatalf("bytes_value = %v", value.GetBytesValue())
	}
}

func TestHomogeneousSlicesMapToArrayValue(t *testing.T) {
	t.Parallel()

	event := convert(snapshot(tracetest.SpanStub{
		Name: "x",
		Attributes: []attribute.KeyValue{
			attribute.StringSlice("tags", []string{"a", "b"}),
			attribute.Int64Slice("counts", []int64{1, 2, 3}),
			attribute.Float64Slice("ratios", []float64{0.5}),
			attribute.BoolSlice("flags", []bool{true, false}),
		},
	}))

	tags := attributeValue(t, event, "tags").GetArrayValue()
	if len(tags.GetValues()) != 2 || tags.GetValues()[1].GetStringValue() != "b" {
		t.Errorf("tags = %v", tags)
	}
	counts := attributeValue(t, event, "counts").GetArrayValue()
	if len(counts.GetValues()) != 3 || counts.GetValues()[2].GetIntegerValue() != 3 {
		t.Errorf("counts = %v", counts)
	}
	ratios := attributeValue(t, event, "ratios").GetArrayValue()
	if len(ratios.GetValues()) != 1 || ratios.GetValues()[0].GetDoubleValue() != 0.5 {
		t.Errorf("ratios = %v", ratios)
	}
	flags := attributeValue(t, event, "flags").GetArrayValue()
	if len(flags.GetValues()) != 2 || !flags.GetValues()[0].GetBoolValue() || flags.GetValues()[1].GetBoolValue() {
		t.Errorf("flags = %v", flags)
	}
}

// TestHeterogeneousSliceMapsToArrayValue is the parity case with span-to-event.ts:144-156,
// whose array mapping recurses and is therefore heterogeneous. attribute.SLICE, added in otel
// v1.44.0, is what makes that expressible in Go at all.
func TestHeterogeneousSliceMapsToArrayValue(t *testing.T) {
	t.Parallel()

	event := convert(snapshot(tracetest.SpanStub{
		Name: "x",
		Attributes: []attribute.KeyValue{attribute.Slice("mixed",
			attribute.StringValue("a"), attribute.Int64Value(7), attribute.BoolValue(true)),
		},
	}))
	values := attributeValue(t, event, "mixed").GetArrayValue().GetValues()
	if len(values) != 3 {
		t.Fatalf("array_value length = %d, want 3", len(values))
	}
	if values[0].GetStringValue() != "a" || values[1].GetIntegerValue() != 7 || !values[2].GetBoolValue() {
		t.Fatalf("heterogeneous members not preserved: %v", values)
	}
}

// TestMapAttributeMapsToMapValue is the parity case with span-to-event.ts:164-178.
// attribute.MAP landed in otel v1.45.0, which is why that is the pinned floor.
func TestMapAttributeMapsToMapValue(t *testing.T) {
	t.Parallel()

	event := convert(snapshot(tracetest.SpanStub{
		Name: "x",
		Attributes: []attribute.KeyValue{attribute.Map("nested",
			attribute.String("inner", "v"),
			attribute.Int64("count", 2),
			attribute.Map("deeper", attribute.Bool("flag", true)),
		)},
	}))
	entries := attributeValue(t, event, "nested").GetMapValue().GetEntries()
	if len(entries) != 3 {
		t.Fatalf("map_value entries = %d, want 3", len(entries))
	}
	// attribute.MapValue canonicalises key order, so look entries up by key rather than by
	// position. Top-level span attributes keep their given order; MAP members do not.
	byKey := func(key string) *modelv1.AttributeValue {
		for _, entry := range entries {
			if entry.GetKey() == key {
				return entry.GetValue()
			}
		}
		t.Fatalf("map entry %q missing from %v", key, entries)
		return nil
	}
	if byKey("inner").GetStringValue() != "v" {
		t.Errorf("inner = %v", byKey("inner"))
	}
	if byKey("count").GetIntegerValue() != 2 {
		t.Errorf("count = %v", byKey("count"))
	}
	deeper := byKey("deeper").GetMapValue().GetEntries()
	if len(deeper) != 1 || !deeper[0].GetValue().GetBoolValue() {
		t.Errorf("nested map not recursed: %v", deeper)
	}
}

func TestEmptySlicesAndMapsMapToEmptyContainers(t *testing.T) {
	t.Parallel()

	event := convert(snapshot(tracetest.SpanStub{
		Name: "x",
		Attributes: []attribute.KeyValue{
			attribute.StringSlice("empty_array", nil),
			attribute.Map("empty_map"),
		},
	}))
	if values := attributeValue(t, event, "empty_array").GetArrayValue(); values == nil || len(values.GetValues()) != 0 {
		t.Errorf("empty slice must still produce an empty array_value, got %v", values)
	}
	if entries := attributeValue(t, event, "empty_map").GetMapValue(); entries == nil || len(entries.GetEntries()) != 0 {
		t.Errorf("empty map must still produce an empty map_value, got %v", entries)
	}
}

// TestEmptyAttributeValueIsSkipped mirrors the reference skipping null and undefined values.
func TestEmptyAttributeValueIsSkipped(t *testing.T) {
	t.Parallel()

	event := convert(snapshot(tracetest.SpanStub{
		Name: "x",
		Attributes: []attribute.KeyValue{
			{Key: "empty", Value: attribute.Value{}},
			attribute.String("kept", "v"),
		},
	}))
	if len(event.GetAttributes()) != 1 || event.GetAttributes()[0].GetKey() != "kept" {
		t.Fatalf("an EMPTY attribute must be skipped, got %v", event.GetAttributes())
	}
}

func TestReservedKeysAreExcludedFromAttributes(t *testing.T) {
	t.Parallel()

	event := convert(snapshot(tracetest.SpanStub{
		Name: "x",
		Attributes: []attribute.KeyValue{
			attribute.String(emission.AttributeEventID, "evt-1"),
			attribute.String(emission.AttributeParentEventID, "evt-0"),
			attribute.String("kept", "v"),
		},
	}))
	for _, entry := range event.GetAttributes() {
		if entry.GetKey() == emission.AttributeEventID || entry.GetKey() == emission.AttributeParentEventID {
			t.Fatalf("reserved key %q leaked into attributes", entry.GetKey())
		}
	}
	if len(event.GetAttributes()) != 1 {
		t.Fatalf("attributes = %v, want only the domain attribute", event.GetAttributes())
	}
}

// TestAttributeOrderIsDeterministic is stronger than the reference's equivalent: OTel Go
// attributes are an ordered slice, not a JavaScript object.
func TestAttributeOrderIsDeterministic(t *testing.T) {
	t.Parallel()

	event := convert(snapshot(tracetest.SpanStub{
		Name: "x",
		Attributes: []attribute.KeyValue{
			attribute.String("z", "1"), attribute.String("a", "2"), attribute.String("m", "3"),
		},
	}))
	var keys []string
	for _, entry := range event.GetAttributes() {
		keys = append(keys, entry.GetKey())
	}
	if !slices.Equal(keys, []string{"z", "a", "m"}) {
		t.Fatalf("attribute order = %v, want the span's own order", keys)
	}
}

func TestParentEventIDComesFromReservedAttribute(t *testing.T) {
	t.Parallel()

	event := convert(snapshot(tracetest.SpanStub{
		Name:       "x",
		Parent:     spanContext(t, "0102030405060708090a0b0c0d0e0f10", "1112131415161718"),
		Attributes: []attribute.KeyValue{attribute.String(emission.AttributeParentEventID, "evt-parent")},
	}))
	if !slices.Equal(event.GetCausalPredecessorIds(), []string{"evt-parent"}) {
		t.Fatalf("causal predecessors = %v, want the parent event id", event.GetCausalPredecessorIds())
	}
}

func TestLinkPredecessorComesFromThatLinksOwnAttributes(t *testing.T) {
	t.Parallel()

	event := convert(snapshot(tracetest.SpanStub{
		Name: "x",
		Links: []sdktrace.Link{{
			SpanContext: spanContext(t, "0102030405060708090a0b0c0d0e0f10", "2122232425262728"),
			Attributes:  []attribute.KeyValue{attribute.String(emission.AttributeEventID, "evt-linked")},
		}},
	}))
	if !slices.Equal(event.GetCausalPredecessorIds(), []string{"evt-linked"}) {
		t.Fatalf("causal predecessors = %v, want the linked event id", event.GetCausalPredecessorIds())
	}
}

// TestParentAndLinkProduceExactlyTheJoinableEdges: parent first, then links in order.
func TestParentAndLinkProduceExactlyTheJoinableEdges(t *testing.T) {
	t.Parallel()

	var malformed []string
	event := emission.SpanToEvent(emission.SpanConversion{
		Span: snapshot(tracetest.SpanStub{
			Name:       "x",
			Parent:     spanContext(t, "0102030405060708090a0b0c0d0e0f10", "1112131415161718"),
			Attributes: []attribute.KeyValue{attribute.String(emission.AttributeParentEventID, "evt-parent")},
			Links: []sdktrace.Link{
				{
					SpanContext: spanContext(t, "0102030405060708090a0b0c0d0e0f10", "2122232425262728"),
					Attributes:  []attribute.KeyValue{attribute.String(emission.AttributeEventID, "evt-second")},
				},
				{
					SpanContext: spanContext(t, "0102030405060708090a0b0c0d0e0f10", "3132333435363738"),
					Attributes:  []attribute.KeyValue{attribute.String(emission.AttributeEventID, "evt-third")},
				},
			},
		}),
		EventID:         "evt-1",
		OnMalformedLink: func(source, spanID string) { malformed = append(malformed, source+":"+spanID) },
	})

	want := []string{"evt-parent", "evt-second", "evt-third"}
	if !slices.Equal(event.GetCausalPredecessorIds(), want) {
		t.Fatalf("causal predecessors = %v, want %v", event.GetCausalPredecessorIds(), want)
	}
	if len(malformed) != 0 {
		t.Fatalf("a well-formed producer must report no malformed links, got %v", malformed)
	}
}

func TestMissingLinkEventIDFallsBackToSpanIDAndReports(t *testing.T) {
	t.Parallel()

	var malformed [][2]string
	event := emission.SpanToEvent(emission.SpanConversion{
		Span: snapshot(tracetest.SpanStub{
			Name: "x",
			Links: []sdktrace.Link{{
				SpanContext: spanContext(t, "0102030405060708090a0b0c0d0e0f10", "2122232425262728"),
			}},
		}),
		OnMalformedLink: func(source, spanID string) { malformed = append(malformed, [2]string{source, spanID}) },
	})

	if !slices.Equal(event.GetCausalPredecessorIds(), []string{"2122232425262728"}) {
		t.Fatalf("causal predecessors = %v, want the hex span id fallback", event.GetCausalPredecessorIds())
	}
	if len(malformed) != 1 || malformed[0] != [2]string{emission.MalformedLinkSourceLink, "2122232425262728"} {
		t.Fatalf("OnMalformedLink calls = %v", malformed)
	}
}

func TestMissingParentEventIDFallsBackToSpanIDAndReports(t *testing.T) {
	t.Parallel()

	var malformed [][2]string
	event := emission.SpanToEvent(emission.SpanConversion{
		Span: snapshot(tracetest.SpanStub{
			Name:   "x",
			Parent: spanContext(t, "0102030405060708090a0b0c0d0e0f10", "1112131415161718"),
		}),
		OnMalformedLink: func(source, spanID string) { malformed = append(malformed, [2]string{source, spanID}) },
	})

	if !slices.Equal(event.GetCausalPredecessorIds(), []string{"1112131415161718"}) {
		t.Fatalf("causal predecessors = %v, want the parent hex span id fallback", event.GetCausalPredecessorIds())
	}
	if len(malformed) != 1 || malformed[0] != [2]string{emission.MalformedLinkSourceParent, "1112131415161718"} {
		t.Fatalf("OnMalformedLink calls = %v", malformed)
	}
}

// TestMistypedReservedAttributeTakesTheFallbackPath: a non-string value under a reserved key
// is treated as absent, matching the reference's typeof guard, rather than being coerced into
// a garbage event ID.
func TestMistypedReservedAttributeTakesTheFallbackPath(t *testing.T) {
	t.Parallel()

	var malformed []string
	event := emission.SpanToEvent(emission.SpanConversion{
		Span: snapshot(tracetest.SpanStub{
			Name:       "x",
			Parent:     spanContext(t, "0102030405060708090a0b0c0d0e0f10", "1112131415161718"),
			Attributes: []attribute.KeyValue{attribute.Int64(emission.AttributeParentEventID, 7)},
		}),
		OnMalformedLink: func(source, _ string) { malformed = append(malformed, source) },
	})

	if !slices.Equal(event.GetCausalPredecessorIds(), []string{"1112131415161718"}) {
		t.Fatalf("causal predecessors = %v", event.GetCausalPredecessorIds())
	}
	if !slices.Equal(malformed, []string{emission.MalformedLinkSourceParent}) {
		t.Fatalf("OnMalformedLink sources = %v", malformed)
	}
	if len(event.GetAttributes()) != 0 {
		t.Fatalf("a reserved key stays excluded even when mistyped, got %v", event.GetAttributes())
	}
}

func TestNoParentAndNoLinksYieldsNoPredecessors(t *testing.T) {
	t.Parallel()

	called := false
	event := emission.SpanToEvent(emission.SpanConversion{
		Span:            snapshot(tracetest.SpanStub{Name: "x"}),
		OnMalformedLink: func(string, string) { called = true },
	})
	if len(event.GetCausalPredecessorIds()) != 0 {
		t.Fatalf("causal predecessors = %v, want none", event.GetCausalPredecessorIds())
	}
	if called {
		t.Fatal("no edges means no malformed-link reports")
	}
}

// TestInvalidParentIsNotAnEdge: ReadOnlySpan.Parent() returns a VALUE, so a root span yields
// a zero trace.SpanContext rather than nil. IsValid() is the correct emptiness test; a nil
// check would not even compile.
func TestInvalidParentIsNotAnEdge(t *testing.T) {
	t.Parallel()

	event := convert(snapshot(tracetest.SpanStub{Name: "x", Parent: trace.SpanContext{}}))
	if len(event.GetCausalPredecessorIds()) != 0 {
		t.Fatalf("a zero-value parent must not become an edge, got %v", event.GetCausalPredecessorIds())
	}
}

func TestNilOnMalformedLinkIsSafe(t *testing.T) {
	t.Parallel()

	event := emission.SpanToEvent(emission.SpanConversion{
		Span: snapshot(tracetest.SpanStub{
			Name:  "x",
			Links: []sdktrace.Link{{SpanContext: spanContext(t, "0102030405060708090a0b0c0d0e0f10", "2122232425262728")}},
		}),
	})
	if len(event.GetCausalPredecessorIds()) != 1 {
		t.Fatalf("the fallback must still be recorded without a callback, got %v", event.GetCausalPredecessorIds())
	}
}

func TestProbeTracerToEventMatchesSpanToEvent(t *testing.T) {
	t.Parallel()

	tracer := emission.NewProbeTracer(emission.Options{TracerName: "probe.test", ServiceName: "sagashop"})
	if tracer.Tracer == nil || tracer.Provider == nil {
		t.Fatal("NewProbeTracer must return a usable tracer and provider")
	}
	t.Cleanup(func() {
		// context.Background(), not t.Context(): the test context is already cancelled by the
		// time cleanup runs, and a registered span processor honours it.
		if err := tracer.Provider.Shutdown(context.Background()); err != nil {
			t.Errorf("shutdown: %v", err)
		}
	})

	conversion := emission.SpanConversion{
		Span:          snapshot(tracetest.SpanStub{Name: "x", Attributes: []attribute.KeyValue{attribute.String("k", "v")}}),
		SchemaVersion: "sentinel.model.v1",
		EventID:       "evt-1",
	}
	viaMethod := tracer.ToEvent(conversion)
	viaFunc := emission.SpanToEvent(conversion)
	if viaMethod.GetKind() != viaFunc.GetKind() || len(viaMethod.GetAttributes()) != len(viaFunc.GetAttributes()) {
		t.Fatal("ToEvent must be a thin wrapper over SpanToEvent")
	}
}

func TestProbeTracerWithoutServiceName(t *testing.T) {
	t.Parallel()

	tracer := emission.NewProbeTracer(emission.Options{TracerName: "probe.test"})
	if tracer.Tracer == nil {
		t.Fatal("an empty ServiceName must still yield a tracer")
	}
	if err := tracer.Provider.Shutdown(context.Background()); err != nil {
		t.Fatalf("shutdown: %v", err)
	}
}

// TestProbeTracerServiceNameMergesOverTheDefaultResource: setting ServiceName must not replace
// the resource. resource.NewWithAttributes on its own would drop telemetry.sdk.* and anything
// the operator set through OTEL_RESOURCE_ATTRIBUTES, which is a poor default for a package
// whose output an Adapter has to normalise.
func TestProbeTracerServiceNameMergesOverTheDefaultResource(t *testing.T) {
	t.Parallel()

	recorder := tracetest.NewSpanRecorder()
	tracer := emission.NewProbeTracer(emission.Options{
		TracerName:      "probe.test",
		ServiceName:     "sagashop-order",
		ProviderOptions: []sdktrace.TracerProviderOption{sdktrace.WithSpanProcessor(recorder)},
	})
	t.Cleanup(func() {
		// context.Background(), not t.Context(): the test context is already cancelled by the
		// time cleanup runs, and a registered span processor honours it.
		if err := tracer.Provider.Shutdown(context.Background()); err != nil {
			t.Errorf("shutdown: %v", err)
		}
	})

	_, span := tracer.Tracer.Start(t.Context(), "order.charge")
	span.End()

	ended := recorder.Ended()
	if len(ended) != 1 {
		t.Fatalf("recorded %d spans, want 1", len(ended))
	}
	attributes := make(map[attribute.Key]attribute.Value)
	for _, kv := range ended[0].Resource().Attributes() {
		attributes[kv.Key] = kv.Value
	}
	if got := attributes["service.name"]; got.AsString() != "sagashop-order" {
		t.Fatalf("service.name = %q, want sagashop-order", got.AsString())
	}
	for _, key := range []attribute.Key{"telemetry.sdk.name", "telemetry.sdk.language", "telemetry.sdk.version"} {
		if _, present := attributes[key]; !present {
			t.Errorf("%s dropped: ServiceName must MERGE over resource.Default(), not replace it", key)
		}
	}
	if ended[0].Resource().SchemaURL() == "" {
		t.Error("the merged resource must keep a schema URL; an empty one means the merge conflicted")
	}
}

// TestProbeTracerDoesNotWriteToTheCallersOptionSlice: appending to a caller-owned slice writes
// into the caller's backing array whenever cap > len, so a host that builds ProviderOptions
// with make([]..., 0, n) and reuses it would see the SDK stomp its array.
func TestProbeTracerDoesNotWriteToTheCallersOptionSlice(t *testing.T) {
	t.Parallel()

	shared := make([]sdktrace.TracerProviderOption, 1, 4)
	shared[0] = sdktrace.WithSpanProcessor(tracetest.NewSpanRecorder())

	tracer := emission.NewProbeTracer(emission.Options{
		TracerName:      "probe.test",
		ServiceName:     "sagashop-order",
		ProviderOptions: shared,
	})
	t.Cleanup(func() {
		// context.Background(), not t.Context(): the test context is already cancelled by the
		// time cleanup runs, and a registered span processor honours it.
		if err := tracer.Provider.Shutdown(context.Background()); err != nil {
			t.Errorf("shutdown: %v", err)
		}
	})

	if len(shared) != 1 {
		t.Fatalf("the caller's slice length changed to %d", len(shared))
	}
	if beyond := shared[:cap(shared)][1]; beyond != nil {
		t.Fatal("the SDK wrote past the caller's length into its backing array")
	}
}

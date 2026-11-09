package otlp_test

import (
	"flag"
	"math"
	"os"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/emet-labs/sentinel/sdk/go/emission"
	modelv1 "github.com/emet-labs/sentinel/sdk/go/gen/sentinel/model/v1"
	"github.com/emet-labs/sentinel/sdk/go/otlp"
	"go.opentelemetry.io/otel/attribute"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"
	coltracepb "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	tracepb "go.opentelemetry.io/proto/otlp/trace/v1"
	"google.golang.org/protobuf/proto"
	"pgregory.net/rapid"
)

func TestMain(m *testing.M) {
	if err := flag.Set("rapid.seed", "1"); err != nil {
		panic(err)
	}
	os.Exit(m.Run())
}

// TestExportMatchesSpanToEventOrRejects draws span attributes with a fixed
// seed. An accepted export's domain attributes and causal event ids match
// SpanToEvent. A predecessor that would fall back to a span id, a non-finite
// double, an unknown sentinel key, or a missing contract attribute is rejected
// and is not encoded.
func TestExportMatchesSpanToEventOrRejects(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		mode := rapid.SampledFrom([]string{
			"well-formed", "missing-parent", "missing-link", "nonfinite", "unknown-sentinel", "missing-contract",
		}).Draw(t, "mode")
		start := time.Unix(1_700_000_000, 0).UTC()
		parent := trace.NewSpanContext(trace.SpanContextConfig{
			TraceID: traceID("0102030405060708090a0b0c0d0e0f10"),
			SpanID:  spanID("1112131415161718"),
		})
		linkID := spanID("2122232425262728")
		attrs := domainAttrs(t)
		var links []sdktrace.Link
		includeParent := false
		switch mode {
		case "well-formed":
			attrs = append(attrs, contractAttrs("evt-self", 1, 4, "")...)
			if rapid.Bool().Draw(t, "parent") {
				includeParent = true
				attrs = append(attrs, attribute.String(emission.AttributeParentEventID, "evt-parent"))
			}
			if rapid.Bool().Draw(t, "link") {
				links = append(links, sdktrace.Link{
					SpanContext: trace.NewSpanContext(trace.SpanContextConfig{
						TraceID: parent.TraceID(),
						SpanID:  linkID,
					}),
					Attributes: []attribute.KeyValue{attribute.String(emission.AttributeEventID, "evt-link")},
				})
			}
		case "missing-parent":
			includeParent = true
			attrs = append(attrs, contractAttrs("evt-self", 1, 4, "")...)
		case "missing-link":
			attrs = append(attrs, contractAttrs("evt-self", 1, 4, "")...)
			links = append(links, sdktrace.Link{
				SpanContext: trace.NewSpanContext(trace.SpanContextConfig{
					TraceID: parent.TraceID(),
					SpanID:  linkID,
				}),
			})
		case "nonfinite":
			attrs = append(attrs, contractAttrs("evt-self", 1, 4, "")...)
			attrs = append(attrs, attribute.Float64("app.ratio", math.NaN()))
		case "unknown-sentinel":
			attrs = append(attrs, contractAttrs("evt-self", 1, 4, "")...)
			attrs = append(attrs, attribute.String("sentinel.not-a-contract", "x"))
		case "missing-contract":
			attrs = append(attrs, attribute.String(emission.AttributeEventID, "evt-self"))
		}

		stub := tracetest.SpanStub{
			Name:       "kind.test",
			StartTime:  start,
			EndTime:    start.Add(time.Millisecond),
			Attributes: attrs,
			Links:      links,
		}
		if includeParent {
			stub.Parent = parent
		}
		span := stub.Snapshot()
		event := emission.SpanToEvent(emission.SpanConversion{
			Span:          span,
			EventID:       "evt-self",
			SchemaVersion: "sagashop-contract-v1",
			Sequence:      &modelv1.SequenceCoordinate{Epoch: 7, Sequence: 1},
		})
		body, err := otlp.BuildExport([]sdktrace.ReadOnlySpan{span})
		if mode != "well-formed" {
			if err == nil {
				t.Fatalf("%s was exported", mode)
			}
			if len(body) != 0 {
				t.Fatal("a refused export must not produce bytes")
			}
			return
		}
		if err != nil {
			t.Fatal(err)
		}
		got := domainFromExport(t, body)
		if !equalEntries(domainOf(event), got) {
			t.Fatalf("domain attributes = %v, spanToEvent = %v", got, domainOf(event))
		}
		causal := causalFromExport(t, body)
		if !slices.Equal(causal, event.GetCausalPredecessorIds()) {
			t.Fatalf("causal ids = %v, spanToEvent = %v", causal, event.GetCausalPredecessorIds())
		}
		for _, id := range causal {
			if id == parent.SpanID().String() || id == linkID.String() {
				t.Fatalf("causal id %q is a span id", id)
			}
		}
	})
}

func domainAttrs(t *rapid.T) []attribute.KeyValue {
	count := rapid.IntRange(0, 4).Draw(t, "count")
	seen := map[string]struct{}{}
	var attrs []attribute.KeyValue
	for range count {
		key := "app." + rapid.StringMatching(`[a-z]{1,6}`).Draw(t, "key")
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		switch rapid.IntRange(0, 3).Draw(t, "type") {
		case 0:
			attrs = append(attrs, attribute.String(key, rapid.StringMatching(`[a-z0-9]{0,8}`).Draw(t, "text")))
		case 1:
			attrs = append(attrs, attribute.Bool(key, rapid.Bool().Draw(t, "flag")))
		case 2:
			attrs = append(attrs, attribute.Int64(key, rapid.Int64().Draw(t, "integer")))
		default:
			attrs = append(attrs, attribute.Float64(key, float64(rapid.Int64Range(-1000, 1000).Draw(t, "finite"))))
		}
	}
	return attrs
}

func domainOf(event *modelv1.ProducerEvent) []*modelv1.AttributeEntry {
	var entries []*modelv1.AttributeEntry
	for _, entry := range event.GetAttributes() {
		if strings.HasPrefix(entry.GetKey(), "sentinel.") {
			continue
		}
		entries = append(entries, entry)
	}
	return entries
}

func domainFromExport(t *rapid.T, body []byte) []*modelv1.AttributeEntry {
	span := onlySpan(t, body)
	var entries []*modelv1.AttributeEntry
	for _, kv := range span.GetAttributes() {
		if strings.HasPrefix(kv.GetKey(), "sentinel.") {
			continue
		}
		entries = append(entries, &modelv1.AttributeEntry{Key: kv.GetKey(), Value: anyToModel(t, kv.GetValue())})
	}
	return entries
}

func causalFromExport(t *rapid.T, body []byte) []string {
	span := onlySpan(t, body)
	var ids []string
	if parent, ok := findAttr(span.GetAttributes(), otlp.AttributeParentEventID); ok {
		ids = append(ids, parent.GetStringValue())
	}
	for _, link := range span.GetLinks() {
		value, ok := findAttr(link.GetAttributes(), otlp.AttributeEventID)
		if !ok {
			t.Fatal("exported link is missing sentinel.event.id")
		}
		ids = append(ids, value.GetStringValue())
	}
	return ids
}

func onlySpan(t *rapid.T, body []byte) *tracepb.Span {
	var request coltracepb.ExportTraceServiceRequest
	if err := proto.Unmarshal(body, &request); err != nil {
		t.Fatal(err)
	}
	spans := allSpans(&request)
	if len(spans) != 1 {
		t.Fatalf("spans = %d", len(spans))
	}
	return spans[0]
}

func anyToModel(t *rapid.T, value *commonpb.AnyValue) *modelv1.AttributeValue {
	switch typed := value.GetValue().(type) {
	case *commonpb.AnyValue_StringValue:
		return &modelv1.AttributeValue{Value: &modelv1.AttributeValue_StringValue{StringValue: typed.StringValue}}
	case *commonpb.AnyValue_BoolValue:
		return &modelv1.AttributeValue{Value: &modelv1.AttributeValue_BoolValue{BoolValue: typed.BoolValue}}
	case *commonpb.AnyValue_IntValue:
		return &modelv1.AttributeValue{Value: &modelv1.AttributeValue_IntegerValue{IntegerValue: typed.IntValue}}
	case *commonpb.AnyValue_DoubleValue:
		return &modelv1.AttributeValue{Value: &modelv1.AttributeValue_DoubleValue{DoubleValue: typed.DoubleValue}}
	default:
		t.Fatalf("domain value %T", value.GetValue())
		return nil
	}
}

func equalEntries(left, right []*modelv1.AttributeEntry) bool {
	if len(left) != len(right) {
		return false
	}
	for i := range left {
		if left[i].GetKey() != right[i].GetKey() || !proto.Equal(left[i].GetValue(), right[i].GetValue()) {
			return false
		}
	}
	return true
}

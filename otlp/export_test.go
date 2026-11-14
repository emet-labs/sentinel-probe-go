package otlp_test

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/emet-labs/sentinel/sdk/go/emission"
	"github.com/emet-labs/sentinel/sdk/go/otlp"
	"go.opentelemetry.io/otel/attribute"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"
	coltracepb "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	"google.golang.org/protobuf/proto"
)

func TestExporterPostsTraceWithSourceHandle(t *testing.T) {
	var header, contentType, path string
	var body []byte
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		header = r.Header.Get(otlp.SourceHandleHeader)
		contentType = r.Header.Get("Content-Type")
		path = r.URL.Path
		var err error
		body, err = io.ReadAll(r.Body)
		if err != nil {
			t.Errorf("read: %v", err)
		}
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(server.Close)

	tracer := emission.NewProbeTracer(emission.Options{
		TracerName:   "probe.test",
		ServiceName:  "sagashop-order",
		Endpoint:     server.URL,
		SourceHandle: "sagashop-order",
		Async:        emission.AsyncOptions{Synchronous: true},
	})
	t.Cleanup(func() {
		if err := tracer.Provider.Shutdown(context.Background()); err != nil {
			t.Errorf("shutdown: %v", err)
		}
	})

	start := time.Unix(1_700_000_000, 0).UTC()
	_, span := tracer.Tracer.Start(context.Background(), "sagashop.order.ship.requested",
		trace.WithTimestamp(start),
		trace.WithAttributes(contractAttrs("01f0c0ffee-ship", 2, 4, "01f0c0ffee-charge")...))
	span.SetAttributes(attribute.String("sagashop.order.id", "order-1001"))
	span.End(trace.WithTimestamp(start.Add(time.Millisecond)))

	if header != "sagashop-order" {
		t.Fatalf("source handle = %q", header)
	}
	if contentType != "application/x-protobuf" {
		t.Fatalf("content type = %q", contentType)
	}
	if path != "/v1/traces" {
		t.Fatalf("path = %q", path)
	}
	var request coltracepb.ExportTraceServiceRequest
	if err := proto.Unmarshal(body, &request); err != nil {
		t.Fatal(err)
	}
	spans := allSpans(&request)
	if len(spans) != 1 {
		t.Fatalf("spans = %d", len(spans))
	}
	if attrString(t, spans[0].GetAttributes(), otlp.AttributeEventID) != "01f0c0ffee-ship" {
		t.Fatal("event id")
	}
	if attrString(t, spans[0].GetAttributes(), otlp.AttributeParentEventID) != "01f0c0ffee-charge" {
		t.Fatal("predecessor must be the event id")
	}
	if attrString(t, spans[0].GetAttributes(), "sagashop.order.id") != "order-1001" {
		t.Fatal("order id")
	}
	var sawService, sawSDK bool
	for _, resource := range request.GetResourceSpans() {
		for _, kv := range resource.GetResource().GetAttributes() {
			if kv.GetKey() == "service.name" && kv.GetValue().GetStringValue() == "sagashop-order" {
				sawService = true
			}
			if kv.GetKey() == "telemetry.sdk.name" {
				sawSDK = true
			}
		}
	}
	if !sawService || !sawSDK {
		t.Fatal("service name must merge over the default resource")
	}
}

func TestExportRefusesSpanIDPredecessor(t *testing.T) {
	posts := 0
	server := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		posts++
	}))
	t.Cleanup(server.Close)
	exporter, err := otlp.NewTraceExporter(otlp.TraceConfig{Endpoint: server.URL, SourceHandle: "sagashop-order"})
	if err != nil {
		t.Fatal(err)
	}
	start := time.Unix(1_700_000_000, 0).UTC()
	span := tracetest.SpanStub{
		Name:      "sagashop.order.ship.requested",
		StartTime: start,
		EndTime:   start.Add(time.Millisecond),
		Parent: trace.NewSpanContext(trace.SpanContextConfig{
			TraceID: traceID("0102030405060708090a0b0c0d0e0f10"),
			SpanID:  spanID("1112131415161718"),
		}),
		Attributes: contractAttrs("01f0c0ffee-ship", 2, 4, ""),
	}.Snapshot()
	err = exporter.ExportSpans(context.Background(), []sdktrace.ReadOnlySpan{span})
	if err == nil {
		t.Fatal("a parent span id without sentinel.parent.event.id must be refused")
	}
	if posts != 0 {
		t.Fatal("a refused span must not be posted")
	}
}

// A remote parent with no sentinel.parent.event.id is a caller that is not a Probe (a browser, a curl). It is
// refused by default, exported with no causal edge under AllowUnattributedRemoteParent, and a LOCAL parent
// without the id stays refused either way: the host owns that edge.
func TestExportUnattributedRemoteParent(t *testing.T) {
	start := time.Unix(1_700_000_000, 0).UTC()
	span := func(remote bool) sdktrace.ReadOnlySpan {
		return tracetest.SpanStub{
			Name:      "sagashop.payment.charge",
			StartTime: start,
			EndTime:   start.Add(time.Millisecond),
			Parent: trace.NewSpanContext(trace.SpanContextConfig{
				TraceID: traceID("0102030405060708090a0b0c0d0e0f10"),
				SpanID:  spanID("1112131415161718"),
				Remote:  remote,
			}),
			Attributes: contractAttrs("01f0c0ffee-charge", 2, 4, ""),
		}.Snapshot()
	}
	for _, tc := range []struct {
		name    string
		remote  bool
		allow   bool
		wantErr bool
	}{
		{"remote refused by default", true, false, true},
		{"remote allowed by option", true, true, false},
		{"local refused despite option", false, true, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			posts := 0
			var gotParentSpanID []byte
			server := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
				posts++
				raw, _ := io.ReadAll(r.Body)
				var req coltracepb.ExportTraceServiceRequest
				if err := proto.Unmarshal(raw, &req); err == nil {
					gotParentSpanID = req.GetResourceSpans()[0].GetScopeSpans()[0].GetSpans()[0].GetParentSpanId()
				}
			}))
			t.Cleanup(server.Close)
			exporter, err := otlp.NewTraceExporter(otlp.TraceConfig{
				Endpoint: server.URL, SourceHandle: "shop-payment",
				Export: otlp.ExportOptions{AllowUnattributedRemoteParent: tc.allow},
			})
			if err != nil {
				t.Fatal(err)
			}
			err = exporter.ExportSpans(context.Background(), []sdktrace.ReadOnlySpan{span(tc.remote)})
			if !tc.wantErr && len(gotParentSpanID) != 0 {
				t.Fatalf("an unattributed remote parent must leave the span a root on the wire, got parent span id %x", gotParentSpanID)
			}
			if (err != nil) != tc.wantErr {
				t.Fatalf("err = %v, wantErr %v", err, tc.wantErr)
			}
			if wantPosts := map[bool]int{true: 0, false: 1}[tc.wantErr]; posts != wantPosts {
				t.Fatalf("posts = %d, want %d", posts, wantPosts)
			}
		})
	}
}

func contractAttrs(eventID string, sequence, filterEpoch int64, parent string) []attribute.KeyValue {
	attrs := []attribute.KeyValue{
		attribute.String(emission.AttributeEventID, eventID),
		attribute.Int64(emission.AttributeSequenceEpoch, 7),
		attribute.Int64(emission.AttributeSequenceNumber, sequence),
		attribute.String(emission.AttributeSchemaVersion, "sagashop-contract-v1"),
		attribute.Int64(emission.AttributeAcknowledgedFilterEpoch, filterEpoch),
		attribute.String(emission.AttributeClaimedSensitivity, "SENSITIVITY_INTERNAL"),
	}
	if parent != "" {
		attrs = append(attrs, attribute.String(emission.AttributeParentEventID, parent))
	}
	return attrs
}

func traceID(hex string) trace.TraceID {
	id, err := trace.TraceIDFromHex(hex)
	if err != nil {
		panic(err)
	}
	return id
}

func spanID(hex string) trace.SpanID {
	id, err := trace.SpanIDFromHex(hex)
	if err != nil {
		panic(err)
	}
	return id
}

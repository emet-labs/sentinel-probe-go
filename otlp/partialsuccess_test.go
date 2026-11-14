package otlp_test

import (
	"context"
	"encoding/hex"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/emet-labs/sentinel/sdk/go/otlp"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
)

func TestRejectedSpansDecodesPartialSuccess(t *testing.T) {
	cases := []struct {
		name, body string
		want       int64
		fails      bool
	}{
		{name: "empty", body: "", want: 0},
		{name: "rejected two", body: "0a020802", want: 2},
		{name: "unknown field skipped, last message wins", body: "48070a0208030a050801120178", want: 1},
		{name: "truncated", body: "0a0508", fails: true},
		{name: "negative", body: "0a0b08ffffffffffffffffff01", fails: true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			body, err := hex.DecodeString(tc.body)
			if err != nil {
				t.Fatal(err)
			}
			got, err := otlp.RejectedSpans(body)
			if tc.fails {
				if err == nil {
					t.Fatalf("decoded %d", got)
				}
				return
			}
			if err != nil || got != tc.want {
				t.Fatalf("got %d, %v; want %d", got, err, tc.want)
			}
		})
	}
}

func TestExportSpansCheckedReturnsSentinelsRejectedCount(t *testing.T) {
	replies := []struct {
		status int
		body   string
	}{{http.StatusOK, "0a020801"}, {http.StatusOK, ""}, {http.StatusConflict, ""}, {http.StatusOK, "0a0508"}}
	next := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		reply := replies[next]
		next++
		body, _ := hex.DecodeString(reply.body)
		w.Header().Set("Content-Type", "application/x-protobuf")
		w.WriteHeader(reply.status)
		_, _ = w.Write(body)
	}))
	t.Cleanup(server.Close)
	exporter, err := otlp.NewTraceExporter(otlp.TraceConfig{Endpoint: server.URL, SourceHandle: "sagashop-order"})
	if err != nil {
		t.Fatal(err)
	}
	spans := []sdktrace.ReadOnlySpan{checkedSpan()}
	if rejected, err := exporter.ExportSpansChecked(context.Background(), spans); err != nil || rejected != 1 {
		t.Fatalf("partial success: %d, %v", rejected, err)
	}
	if rejected, err := exporter.ExportSpansChecked(context.Background(), spans); err != nil || rejected != 0 {
		t.Fatalf("clean 200: %d, %v", rejected, err)
	}
	if _, err := exporter.ExportSpansChecked(context.Background(), spans); err == nil {
		t.Fatal("409 accepted")
	}
	if _, err := exporter.ExportSpansChecked(context.Background(), spans); err == nil {
		t.Fatal("undecodable 200 accepted")
	}
}

func checkedSpan() sdktrace.ReadOnlySpan {
	start := time.Unix(1_700_000_000, 0).UTC()
	return tracetest.SpanStub{
		Name:       "sagashop.order.ship.requested",
		StartTime:  start,
		EndTime:    start.Add(time.Millisecond),
		Attributes: contractAttrs("01f0c0ffee-ship", 2, 4, ""),
	}.Snapshot()
}

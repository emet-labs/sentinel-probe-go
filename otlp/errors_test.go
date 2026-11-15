package otlp_test

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"

	"github.com/emet-labs/sentinel-probe-go/otlp"
)

func validSpan() sdktrace.ReadOnlySpan {
	start := time.Unix(1_700_000_000, 0).UTC()
	return tracetest.SpanStub{
		Name: "shop.payment.charge", StartTime: start, EndTime: start.Add(time.Millisecond),
		Attributes: contractAttrs("shop-payment:1", 1, 0, ""),
	}.Snapshot()
}

func TestExportErrorCarriesTheResponseBody(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "unknown source handle shop-payment", http.StatusForbidden)
	}))
	defer server.Close()
	exporter, err := otlp.NewTraceExporter(otlp.TraceConfig{Endpoint: server.URL, SourceHandle: "shop-payment"})
	if err != nil {
		t.Fatal(err)
	}
	for name, export := range map[string]func() error{
		"ExportSpans": func() error { return exporter.ExportSpans(context.Background(), []sdktrace.ReadOnlySpan{validSpan()}) },
		"Checked": func() error {
			_, err := exporter.ExportSpansChecked(context.Background(), []sdktrace.ReadOnlySpan{validSpan()})
			return err
		},
	} {
		err := export()
		var status *otlp.HTTPStatusError
		if !errors.As(err, &status) || status.StatusCode != http.StatusForbidden {
			t.Fatalf("%s: %v", name, err)
		}
		if !strings.Contains(err.Error(), "unknown source handle shop-payment") || !strings.Contains(status.Body, "unknown source handle") {
			t.Fatalf("%s: body missing: %v", name, err)
		}
		var refusal *otlp.Refusal
		if !errors.As(err, &refusal) || refusal.Reason != "export-refused" {
			t.Fatalf("%s: no longer a Refusal: %v", name, err)
		}
		if otlp.Retryable(err) {
			t.Fatalf("%s: a 403 must not be retryable", name)
		}
	}
}

func TestRetryableClassification(t *testing.T) {
	for status, want := range map[int]bool{
		400: false, 401: false, 403: false, 404: false, 409: false, 501: false, 505: false,
		408: true, 425: true, 429: true, 500: true, 502: true, 503: true, 504: true,
	} {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(status) }))
		exporter, _ := otlp.NewTraceExporter(otlp.TraceConfig{Endpoint: server.URL, SourceHandle: "h"})
		err := exporter.ExportSpans(context.Background(), []sdktrace.ReadOnlySpan{validSpan()})
		server.Close()
		if got := otlp.Retryable(err); got != want {
			t.Errorf("status %d: Retryable = %v, want %v", status, got, want)
		}
	}
	// A dead endpoint is a transport error and retryable; a contract refusal is not.
	dead := httptest.NewServer(http.NotFoundHandler())
	url := dead.URL
	dead.Close()
	exporter, _ := otlp.NewTraceExporter(otlp.TraceConfig{Endpoint: url, SourceHandle: "h"})
	err := exporter.ExportSpans(context.Background(), []sdktrace.ReadOnlySpan{validSpan()})
	var transport *otlp.TransportError
	if !errors.As(err, &transport) || !otlp.Retryable(err) {
		t.Fatalf("dead endpoint: %v", err)
	}
	bad := tracetest.SpanStub{Name: "x", StartTime: time.Unix(1, 0), EndTime: time.Unix(2, 0)}.Snapshot()
	err = exporter.ExportSpans(context.Background(), []sdktrace.ReadOnlySpan{bad})
	if err == nil || otlp.Retryable(err) {
		t.Fatalf("contract refusal: %v", err)
	}
}

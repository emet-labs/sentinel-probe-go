package otlp

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"sync"

	sdktrace "go.opentelemetry.io/otel/sdk/trace"
)

// TraceExporter posts ended spans to a Sentinel OTLP/HTTP trace endpoint.
type TraceExporter struct {
	url          string
	sourceHandle string
	client       *http.Client

	mu     sync.Mutex
	closed bool
}

// TraceConfig selects the Sentinel base URL and the source handle stamped on
// every export. Endpoint is a base such as http://127.0.0.1:8080. The exporter
// posts to /v1/traces.
type TraceConfig struct {
	Endpoint     string
	SourceHandle string
	HTTPClient   *http.Client
}

// NewTraceExporter builds an exporter. A blank handle or a non-HTTP endpoint
// is refused. The exporter does not dial Decide.
func NewTraceExporter(config TraceConfig) (*TraceExporter, error) {
	if strings.TrimSpace(config.SourceHandle) == "" {
		return nil, refuse("missing-source-handle", "")
	}
	target, err := endpointURL(config.Endpoint, "/v1/traces")
	if err != nil {
		return nil, err
	}
	client := config.HTTPClient
	if client == nil {
		client = &http.Client{Timeout: exportTimeout}
	}
	return &TraceExporter{url: target, sourceHandle: config.SourceHandle, client: client}, nil
}

// ExportSpans validates the batch and posts it. One refused span posts nothing.
func (e *TraceExporter) ExportSpans(ctx context.Context, spans []sdktrace.ReadOnlySpan) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	e.mu.Lock()
	closed := e.closed
	e.mu.Unlock()
	if closed {
		return refuse("exporter-closed", "")
	}
	body, err := BuildExport(spans)
	if err != nil || len(body) == 0 {
		return err
	}
	return postProtobuf(ctx, e.client, e.url, e.sourceHandle, body)
}

// Shutdown stops later exports. It does not read a clock beyond the context.
func (e *TraceExporter) Shutdown(context.Context) error {
	e.mu.Lock()
	e.closed = true
	e.mu.Unlock()
	return nil
}

func endpointURL(endpoint, suffix string) (string, error) {
	parsed, err := url.Parse(endpoint)
	if err != nil || parsed.Host == "" || (parsed.Scheme != "http" && parsed.Scheme != "https") {
		return "", refuse("invalid-endpoint", endpoint)
	}
	path := strings.TrimRight(parsed.Path, "/")
	if !strings.HasSuffix(path, suffix) {
		path += suffix
	}
	parsed.Path = path
	parsed.RawQuery = ""
	parsed.Fragment = ""
	return parsed.String(), nil
}

func postProtobuf(ctx context.Context, client *http.Client, target, sourceHandle string, body []byte) error {
	if client == nil {
		client = &http.Client{Timeout: exportTimeout}
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, target, bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("otlp: build request: %w", err)
	}
	request.Header.Set("Content-Type", protobufMediaType)
	request.Header.Set(SourceHandleHeader, sourceHandle)
	response, err := client.Do(request)
	if err != nil {
		return fmt.Errorf("otlp: post: %w", err)
	}
	defer response.Body.Close()
	_, _ = io.Copy(io.Discard, io.LimitReader(response.Body, 1<<20))
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		return refuse("export-refused", response.Status)
	}
	return nil
}

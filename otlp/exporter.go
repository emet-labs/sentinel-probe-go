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
// It does not read Sentinel's answer; ExportSpansChecked does.
func (e *TraceExporter) ExportSpans(ctx context.Context, spans []sdktrace.ReadOnlySpan) error {
	_, err := e.export(ctx, spans, false)
	return err
}

// ExportSpansChecked is ExportSpans that also reads Sentinel's answer and
// returns its partial_success.rejected_spans (#461). An export succeeded only
// when err is nil and rejected is 0. A 2xx answer whose body does not decode
// is an error: Sentinel's verdict on the batch is unknown.
func (e *TraceExporter) ExportSpansChecked(ctx context.Context, spans []sdktrace.ReadOnlySpan) (rejected int64, err error) {
	answer, err := e.export(ctx, spans, true)
	if err != nil || answer == nil {
		return 0, err
	}
	return RejectedSpans(answer)
}

// export posts the batch. With read, it returns the response body, and a nil
// body means nothing was posted; without, it discards the body as it always has.
func (e *TraceExporter) export(ctx context.Context, spans []sdktrace.ReadOnlySpan, read bool) ([]byte, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	e.mu.Lock()
	closed := e.closed
	e.mu.Unlock()
	if closed {
		return nil, refuse("exporter-closed", "")
	}
	body, err := BuildExport(spans)
	if err != nil || len(body) == 0 {
		return nil, err
	}
	if !read {
		return nil, postProtobuf(ctx, e.client, e.url, e.sourceHandle, body)
	}
	return postProtobufBody(ctx, e.client, e.url, e.sourceHandle, body)
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

// postProtobufBody is postProtobuf that returns the 2xx response body, read
// in full (up to 1 MiB). A body that cannot be read is an error. The result is
// never nil on success.
func postProtobufBody(ctx context.Context, client *http.Client, target, sourceHandle string, body []byte) ([]byte, error) {
	if client == nil {
		client = &http.Client{Timeout: exportTimeout}
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, target, bytes.NewReader(body))
	if err != nil {
		return nil, fmt.Errorf("otlp: build request: %w", err)
	}
	request.Header.Set("Content-Type", protobufMediaType)
	request.Header.Set(SourceHandleHeader, sourceHandle)
	response, err := client.Do(request)
	if err != nil {
		return nil, fmt.Errorf("otlp: post: %w", err)
	}
	defer response.Body.Close()
	answer, readErr := io.ReadAll(io.LimitReader(response.Body, 1<<20))
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		return nil, refuse("export-refused", response.Status)
	}
	if readErr != nil {
		return nil, fmt.Errorf("otlp: read response: %w", readErr)
	}
	if answer == nil {
		answer = []byte{}
	}
	return answer, nil
}

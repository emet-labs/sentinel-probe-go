package emission

import (
	"context"
	"net/http"
	"slices"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.43.0"
	"go.opentelemetry.io/otel/trace"

	modelv1 "github.com/emet-labs/sentinel-probe-go/gen/sentinel/model/v1"
	"github.com/emet-labs/sentinel-probe-go/otlp"
)

// Options configures the Probe's tracer. Analog of otel-bridge.ts's EmissionOptions.
type Options struct {
	// TracerName identifies the instrumentation scope, for example the Probe's package path.
	TracerName string
	// ServiceName populates the resource's service.name, MERGED over resource.Default() so
	// telemetry.sdk.* and anything the operator set through OTEL_RESOURCE_ATTRIBUTES survive.
	// Empty means no resource option is added at all, leaving OTel's default in place.
	ServiceName string
	// ProviderOptions are appended to the TracerProvider construction, letting a host attach
	// span processors, samplers or its own resource. The slice is cloned before use, so the
	// caller's backing array is never written to.
	ProviderOptions []sdktrace.TracerProviderOption
	// Endpoint is the Sentinel base URL, for example http://127.0.0.1:8080.
	// When set, ended spans are exported as OTLP/HTTP protobuf to /v1/traces.
	// Empty leaves export unset so a test can attach its own processor.
	// ADR-0002 left export to the host; #398 excepts the Probe wire.
	Endpoint string
	// SourceHandle is sent as x-sentinel-source-handle. Required when Endpoint is set.
	SourceHandle string
	// Sequence is the source's allocator (#461). With it, each span gets
	// sentinel.sequence.epoch and sentinel.sequence.number when it starts,
	// unless it already carries both, and the exporter marks the slot from
	// Sentinel's answer: exported only on HTTP 2xx with zero rejected spans,
	// failed otherwise. Requires Endpoint. Pass the sequence attributes at
	// start or not at all: a slot stamped at start and then overwritten is
	// never marked and holds its epoch open.
	Sequence *SequenceAllocator
	// Export relaxes the span contract where a strict refusal would block a mixed deployment, for example
	// AllowUnattributedRemoteParent for a service whose callers are not all Probes. Requires Endpoint.
	Export otlp.ExportOptions
	// Async tunes the default exporter: a bounded queue, batching, retry with backoff, and a retry buffer for
	// FAILED slots. See AsyncOptions for the overflow, retry and duplicate-Event-id policies. Requires Endpoint.
	Async AsyncOptions
	// OnError receives every *ExportError: a dropped, failed, rejected or panicked export. It is called from the
	// exporter's goroutine (or from span.End() for a drop) and must not block. A panic in it is absorbed.
	OnError func(error)
	// ErrorLogInterval rate-limits the default log of export errors to one line per interval (default 10 s). Set
	// Logf to redirect the log; a negative interval turns the log off.
	ErrorLogInterval time.Duration
	// Logf is the log function for export errors. Defaults to log.Printf.
	Logf func(format string, args ...any)
	// HTTPClient posts to Sentinel. Default: a client with a 5 s timeout. The timeout bounds one request on the
	// exporter's goroutine, not span.End().
	HTTPClient *http.Client
	// HostStamp, when set, stamps the host-owned reserved attributes at span start; see HostStamper. Leave it nil
	// to set them yourself on each span.
	HostStamp *HostStamperOptions
}

// ProbeTracer bundles the tracer a Probe emits with, its provider, and the conversion into
// ProducerEvents. Analog of otel-bridge.ts's ProbeTracer.
type ProbeTracer struct {
	Tracer   trace.Tracer
	Provider *sdktrace.TracerProvider
	// Exporter is the default span processor; nil when no Endpoint was set.
	Exporter *AsyncProcessor
}

// Shutdown flushes the exporter and stops the provider. Call it before the final completeness heartbeat, so
// the heartbeat declares what was actually exported.
func (p *ProbeTracer) Shutdown(ctx context.Context) error {
	return p.Provider.Shutdown(ctx)
}

// NewProbeTracer builds a TracerProvider and a tracer for a Probe.
//
// When Endpoint is set, the provider exports ended spans to that Sentinel as
// OTLP/HTTP protobuf. That is the #398 Probe wire, the exception to ADR-0002's
// rule that the host owns export. A blank Endpoint still installs no exporter.
// A set Endpoint with a blank handle or a non-HTTP URL panics: that is a wiring
// bug, and silently dropping the export would look like a missing span.
//
// The caller owns the returned Provider's lifecycle and must call Shutdown on it.
func NewProbeTracer(options Options) *ProbeTracer {
	// Clone rather than append in place: appending to a caller-owned slice writes into the
	// caller's backing array whenever cap > len, so a host that builds ProviderOptions with
	// make([]..., 0, n) and reuses it would see the SDK stomp its array.
	providerOptions := slices.Clone(options.ProviderOptions)

	if options.ServiceName != "" {
		// MERGE over resource.Default() rather than replacing it. NewWithAttributes alone
		// would silently drop telemetry.sdk.* and anything the operator set through
		// OTEL_RESOURCE_ATTRIBUTES, which is a poor default for a package whose job is
		// emitting spans an Adapter has to normalise.
		merged, err := resource.Merge(
			resource.Default(),
			resource.NewWithAttributes(semconv.SchemaURL, semconv.ServiceName(options.ServiceName)),
		)
		if err != nil {
			// The only error class is a schema-URL conflict, i.e. this package's semconv
			// version has drifted from the one resource.Default() uses. Merge still returns a
			// usable schemaless resource carrying both attribute sets, so use it — but report
			// the drift through OTel's error handler instead of swallowing it.
			otel.Handle(err)
		}
		providerOptions = append(providerOptions, sdktrace.WithResource(merged))
	}

	if options.Sequence != nil && options.Endpoint == "" {
		panic("emission: Sequence requires Endpoint: nothing would mark its slots")
	}
	if options.HostStamp != nil {
		providerOptions = append(providerOptions, sdktrace.WithSpanProcessor(NewHostStamper(*options.HostStamp)))
	}
	var async *AsyncProcessor
	if options.Endpoint != "" {
		exporter, err := otlp.NewTraceExporter(otlp.TraceConfig{
			Endpoint:     options.Endpoint,
			SourceHandle: options.SourceHandle,
			Export:       options.Export,
			HTTPClient:   options.HTTPClient,
		})
		if err != nil {
			panic("emission: OTLP exporter: " + err.Error())
		}
		if options.Sequence != nil {
			// The stamper runs before the exporting processor, so every span
			// it exports already carries its slot.
			providerOptions = append(providerOptions, sdktrace.WithSpanProcessor(sequenceStamper{allocator: options.Sequence}))
		}
		var logger *rateLimitedLog
		if options.ErrorLogInterval >= 0 {
			logger = newRateLimitedLog(options.ErrorLogInterval, options.Logf)
		}
		async = newAsyncProcessor(exporter, options.Sequence, options.Async, options.OnError, logger)
		providerOptions = append(providerOptions, sdktrace.WithSpanProcessor(async))
	}

	provider := sdktrace.NewTracerProvider(providerOptions...)
	return &ProbeTracer{
		Tracer:   provider.Tracer(options.TracerName),
		Provider: provider,
		Exporter: async,
	}
}

// ToEvent converts an ended span into a ProducerEvent. Convenience wrapper over SpanToEvent,
// mirroring the reference's ProbeTracer.toEvent.
func (p *ProbeTracer) ToEvent(conversion SpanConversion) *modelv1.ProducerEvent {
	return SpanToEvent(conversion)
}

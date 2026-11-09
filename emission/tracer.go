package emission

import (
	"slices"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.43.0"
	"go.opentelemetry.io/otel/trace"

	modelv1 "github.com/emet-labs/sentinel/sdk/go/gen/sentinel/model/v1"
	"github.com/emet-labs/sentinel/sdk/go/otlp"
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
}

// ProbeTracer bundles the tracer a Probe emits with, its provider, and the conversion into
// ProducerEvents. Analog of otel-bridge.ts's ProbeTracer.
type ProbeTracer struct {
	Tracer   trace.Tracer
	Provider *sdktrace.TracerProvider
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

	if options.Endpoint != "" {
		exporter, err := otlp.NewTraceExporter(otlp.TraceConfig{
			Endpoint:     options.Endpoint,
			SourceHandle: options.SourceHandle,
		})
		if err != nil {
			panic("emission: OTLP exporter: " + err.Error())
		}
		providerOptions = append(providerOptions, sdktrace.WithSpanProcessor(sdktrace.NewSimpleSpanProcessor(exporter)))
	}

	provider := sdktrace.NewTracerProvider(providerOptions...)
	return &ProbeTracer{
		Tracer:   provider.Tracer(options.TracerName),
		Provider: provider,
	}
}

// ToEvent converts an ended span into a ProducerEvent. Convenience wrapper over SpanToEvent,
// mirroring the reference's ProbeTracer.toEvent.
func (p *ProbeTracer) ToEvent(conversion SpanConversion) *modelv1.ProducerEvent {
	return SpanToEvent(conversion)
}

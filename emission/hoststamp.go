package emission

import (
	"context"
	"fmt"

	"github.com/google/uuid"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"

	"github.com/emet-labs/sentinel-probe-go/otlp"
	"github.com/emet-labs/sentinel-probe-go/propagate"
)

// HostStamperOptions configures NewHostStamper.
type HostStamperOptions struct {
	// SourceHandle prefixes generated Event ids ("<handle>:<uuid>"). Required.
	SourceHandle string
	// SchemaVersion is sentinel.schema.version, the contract the Events conform to. Required.
	SchemaVersion string
	// ClaimedSensitivity is the proto enum name stamped as sentinel.claimed_sensitivity, for example
	// "SENSITIVITY_INTERNAL". Empty means SENSITIVITY_INTERNAL.
	ClaimedSensitivity string
	// FilterEpoch returns the acknowledged Filter epoch, or nil for none (stamped 0).
	// (*client.FilterStore).Epoch fits.
	FilterEpoch func() *uint64
}

// hostStamper is the span processor behind NewHostStamper.
type hostStamper struct{ options HostStamperOptions }

// NewHostStamper returns a span processor that sets, when a span starts, the five reserved attributes the
// producer contract leaves to the host, so a service does not repeat that code per span:
//
//   - sentinel.event.id: "<handle>:<uuid>", unless the span already carries one (a host that owns its ids,
//     such as "<event>:<subject id>", sets it in the start options and wins);
//   - sentinel.parent.event.id: the parent span's Event id in this process, or the id a remote caller sent
//     (propagate.Propagator keeps it in the context), unless the span already carries one;
//   - sentinel.schema.version, sentinel.acknowledged_filter_epoch and sentinel.claimed_sensitivity: unless set.
//
// It never overwrites an attribute the host set, so the explicit path keeps working, and a panic in it is
// absorbed. Register it with Options.HostStamp, or with sdktrace.WithSpanProcessor ahead of the exporter.
func NewHostStamper(options HostStamperOptions) sdktrace.SpanProcessor {
	if options.ClaimedSensitivity == "" {
		options.ClaimedSensitivity = "SENSITIVITY_INTERNAL"
	}
	return hostStamper{options: options}
}

func (s hostStamper) OnStart(parent context.Context, span sdktrace.ReadWriteSpan) {
	defer func() {
		if r := recover(); r != nil {
			otel.Handle(fmt.Errorf("emission: host stamper: panic: %v", r))
		}
	}()
	var hasID, hasParent, hasSchema, hasFilter, hasSensitivity bool
	for _, kv := range span.Attributes() {
		switch string(kv.Key) {
		case otlp.AttributeEventID:
			hasID = true
		case otlp.AttributeParentEventID:
			hasParent = true
		case otlp.AttributeSchemaVersion:
			hasSchema = true
		case otlp.AttributeAcknowledgedFilterEpoch:
			hasFilter = true
		case otlp.AttributeClaimedSensitivity:
			hasSensitivity = true
		}
	}
	set := make([]attribute.KeyValue, 0, 5)
	if !hasID {
		set = append(set, attribute.String(otlp.AttributeEventID, s.options.SourceHandle+":"+uuid.NewString()))
	}
	if !hasParent {
		if id := propagate.ParentEventIDFor(parent); id != "" {
			set = append(set, attribute.String(otlp.AttributeParentEventID, id))
		}
	}
	if !hasSchema {
		set = append(set, attribute.String(otlp.AttributeSchemaVersion, s.options.SchemaVersion))
	}
	if !hasFilter {
		var epoch int64
		if s.options.FilterEpoch != nil {
			if e := s.options.FilterEpoch(); e != nil && *e <= SequenceMax {
				epoch = int64(*e)
			}
		}
		set = append(set, attribute.Int64(otlp.AttributeAcknowledgedFilterEpoch, epoch))
	}
	if !hasSensitivity {
		set = append(set, attribute.String(otlp.AttributeClaimedSensitivity, s.options.ClaimedSensitivity))
	}
	span.SetAttributes(set...)
}

func (hostStamper) OnEnd(sdktrace.ReadOnlySpan)      {}
func (hostStamper) Shutdown(context.Context) error   { return nil }
func (hostStamper) ForceFlush(context.Context) error { return nil }

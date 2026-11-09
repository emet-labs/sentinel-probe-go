package otlp

import (
	"math"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/sdk/instrumentation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"
	coltracepb "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	resourcepb "go.opentelemetry.io/proto/otlp/resource/v1"
	tracepb "go.opentelemetry.io/proto/otlp/trace/v1"
	"google.golang.org/protobuf/proto"
)

// BuildExport encodes ended spans as one uncompressed ExportTraceServiceRequest.
//
// A span that has not ended, drops attributes, links, or events, carries a
// non-finite double, or would use a span id as a causal predecessor is refused.
// Nothing is encoded in that case. Required contract attributes are
// sentinel.event.id, sentinel.sequence.epoch, sentinel.sequence.number,
// sentinel.schema.version, sentinel.acknowledged_filter_epoch, and a
// sentinel.claimed_sensitivity proto enum name other than UNSPECIFIED.
func BuildExport(spans []sdktrace.ReadOnlySpan) ([]byte, error) {
	if len(spans) == 0 {
		return nil, nil
	}
	encoded := make([]*tracepb.ResourceSpans, 0, len(spans))
	for _, span := range spans {
		resourceSpans, err := encodeSpan(span)
		if err != nil {
			return nil, err
		}
		encoded = append(encoded, resourceSpans)
	}
	return proto.Marshal(&coltracepb.ExportTraceServiceRequest{ResourceSpans: encoded})
}

func encodeSpan(span sdktrace.ReadOnlySpan) (*tracepb.ResourceSpans, error) {
	if span.EndTime().IsZero() {
		return nil, refuse("span-not-ended", span.Name())
	}
	start, ok := unixNano(span.StartTime())
	if !ok {
		return nil, refuse("span-start", span.Name())
	}
	end, ok := unixNano(span.EndTime())
	if !ok {
		return nil, refuse("span-end", span.Name())
	}
	if end < start {
		return nil, refuse("end-before-start", span.Name())
	}
	if span.DroppedAttributes() != 0 {
		return nil, refuse("dropped-attributes", span.Name())
	}
	if span.DroppedLinks() != 0 {
		return nil, refuse("dropped-links", span.Name())
	}
	if span.DroppedEvents() != 0 {
		return nil, refuse("dropped-events", span.Name())
	}

	events, err := encodeEvents(span.Events())
	if err != nil {
		return nil, err
	}
	links, err := encodeLinks(span.Links())
	if err != nil {
		return nil, err
	}
	attributes, fields, err := encodeSpanAttributes(span.Attributes())
	if err != nil {
		return nil, err
	}
	if span.Parent().IsValid() && fields.parentEventID == "" {
		return nil, refuse("missing-parent-event-id", span.Parent().SpanID().String())
	}
	if err := fields.require(); err != nil {
		return nil, err
	}

	resourceAttrs, schemaURL, err := encodeResource(span.Resource())
	if err != nil {
		return nil, err
	}
	scope, err := encodeScope(span.InstrumentationScope())
	if err != nil {
		return nil, err
	}

	var parentSpanID []byte
	if span.Parent().IsValid() {
		id := span.Parent().SpanID()
		parentSpanID = id[:]
	}
	spanContext := span.SpanContext()
	traceID := spanContext.TraceID()
	spanID := spanContext.SpanID()

	return &tracepb.ResourceSpans{
		Resource: &resourcepb.Resource{Attributes: resourceAttrs},
		ScopeSpans: []*tracepb.ScopeSpans{{
			Scope: scope,
			Spans: []*tracepb.Span{{
				TraceId:                traceID[:],
				SpanId:                 spanID[:],
				TraceState:             spanContext.TraceState().String(),
				ParentSpanId:           parentSpanID,
				Flags:                  uint32(spanContext.TraceFlags()),
				Name:                   span.Name(),
				Kind:                   spanKind(span.SpanKind()),
				StartTimeUnixNano:      start,
				EndTimeUnixNano:        end,
				Attributes:             attributes,
				Events:                 events,
				Links:                  links,
				DroppedAttributesCount: 0,
				DroppedEventsCount:     0,
				DroppedLinksCount:      0,
			}},
		}},
		SchemaUrl: schemaURL,
	}, nil
}

type spanFields struct {
	eventID       string
	parentEventID string
	sequenceEpoch *uint64
	sequence      *uint64
	schema        string
	filterEpoch   *uint64
	sensitivity   string
}

func (f spanFields) require() error {
	if f.eventID == "" {
		return refuse("missing-event-id", AttributeEventID)
	}
	if f.sequenceEpoch == nil {
		return refuse("missing-sequence-epoch", AttributeSequenceEpoch)
	}
	if f.sequence == nil {
		return refuse("missing-sequence-number", AttributeSequenceNumber)
	}
	if f.schema == "" {
		return refuse("missing-schema-version", AttributeSchemaVersion)
	}
	if f.filterEpoch == nil {
		return refuse("missing-filter-epoch", AttributeAcknowledgedFilterEpoch)
	}
	switch f.sensitivity {
	case "SENSITIVITY_PUBLIC", "SENSITIVITY_INTERNAL", "SENSITIVITY_CONFIDENTIAL", "SENSITIVITY_RESTRICTED":
		return nil
	case "", "SENSITIVITY_UNSPECIFIED":
		return refuse("missing-sensitivity", AttributeClaimedSensitivity)
	default:
		return refuse("invalid-sensitivity", f.sensitivity)
	}
}

func encodeSpanAttributes(attributes []attribute.KeyValue) ([]*commonpb.KeyValue, spanFields, error) {
	var fields spanFields
	seen := make(map[string]struct{}, len(attributes))
	out := make([]*commonpb.KeyValue, 0, len(attributes))
	for _, kv := range attributes {
		key := string(kv.Key)
		if _, ok := seen[key]; ok {
			return nil, fields, refuse("duplicate-attribute", key)
		}
		seen[key] = struct{}{}
		if isSentinel(key) {
			if err := applyReserved(&fields, key, kv.Value); err != nil {
				return nil, fields, err
			}
		}
		encoded, err := convertValue(key, kv.Value)
		if err != nil {
			return nil, fields, err
		}
		if encoded == nil {
			continue
		}
		out = append(out, &commonpb.KeyValue{Key: key, Value: encoded})
	}
	return out, fields, nil
}

func applyReserved(fields *spanFields, key string, value attribute.Value) error {
	switch key {
	case AttributeEventID:
		text, err := requireString(key, value)
		if err != nil {
			return err
		}
		if text == "" || onlySpace(text) {
			return refuse("invalid-event-id", key)
		}
		fields.eventID = text
	case AttributeParentEventID:
		text, err := requireString(key, value)
		if err != nil {
			return err
		}
		if text == "" || onlySpace(text) {
			return refuse("invalid-parent-event-id", key)
		}
		fields.parentEventID = text
	case AttributeSequenceEpoch:
		n, err := requireUint(key, value)
		if err != nil {
			return err
		}
		fields.sequenceEpoch = &n
	case AttributeSequenceNumber:
		n, err := requireUint(key, value)
		if err != nil {
			return err
		}
		fields.sequence = &n
	case AttributeSchemaVersion:
		text, err := requireString(key, value)
		if err != nil {
			return err
		}
		if text == "" || onlySpace(text) {
			return refuse("invalid-schema-version", key)
		}
		fields.schema = text
	case AttributeAcknowledgedFilterEpoch:
		n, err := requireUint(key, value)
		if err != nil {
			return err
		}
		fields.filterEpoch = &n
	case AttributeClaimedSensitivity:
		text, err := requireString(key, value)
		if err != nil {
			return err
		}
		fields.sensitivity = text
	case AttributeClaimedCapabilities:
		if value.Type() != attribute.STRINGSLICE {
			return refuse("invalid-capabilities", key)
		}
		for _, name := range value.AsStringSlice() {
			if !knownCapability(name) {
				return refuse("invalid-capabilities", name)
			}
		}
	default:
		return refuse("unknown-sentinel-attribute", key)
	}
	return nil
}

func knownCapability(name string) bool {
	switch name {
	case "SOURCE_CAPABILITY_OBSERVE_BEFORE_EFFECT",
		"SOURCE_CAPABILITY_CAUSAL_EDGES",
		"SOURCE_CAPABILITY_COMPLETENESS_SIGNALS",
		"SOURCE_CAPABILITY_BOUNDED_CLOCK_UNCERTAINTY":
		return true
	default:
		return false
	}
}

func encodeLinks(links []sdktrace.Link) ([]*tracepb.Span_Link, error) {
	out := make([]*tracepb.Span_Link, 0, len(links))
	for _, link := range links {
		if link.DroppedAttributeCount != 0 {
			return nil, refuse("dropped-link-attributes", "")
		}
		attributes, eventID, err := encodeLinkAttributes(link.Attributes)
		if err != nil {
			return nil, err
		}
		if eventID == "" {
			return nil, refuse("missing-link-event-id", link.SpanContext.SpanID().String())
		}
		traceID := link.SpanContext.TraceID()
		spanID := link.SpanContext.SpanID()
		out = append(out, &tracepb.Span_Link{
			TraceId:    traceID[:],
			SpanId:     spanID[:],
			TraceState: link.SpanContext.TraceState().String(),
			Attributes: attributes,
			Flags:      uint32(link.SpanContext.TraceFlags()),
		})
	}
	return out, nil
}

func encodeLinkAttributes(attributes []attribute.KeyValue) ([]*commonpb.KeyValue, string, error) {
	seen := make(map[string]struct{}, len(attributes))
	var eventID string
	out := make([]*commonpb.KeyValue, 0, len(attributes))
	for _, kv := range attributes {
		key := string(kv.Key)
		if _, ok := seen[key]; ok {
			return nil, "", refuse("duplicate-attribute", key)
		}
		seen[key] = struct{}{}
		if isSentinel(key) && key != AttributeEventID {
			return nil, "", refuse("unknown-sentinel-attribute", key)
		}
		if key == AttributeEventID {
			text, err := requireString(key, kv.Value)
			if err != nil {
				return nil, "", err
			}
			if text == "" || onlySpace(text) {
				return nil, "", refuse("invalid-event-id", key)
			}
			eventID = text
		}
		encoded, err := convertValue(key, kv.Value)
		if err != nil {
			return nil, "", err
		}
		if encoded == nil {
			continue
		}
		out = append(out, &commonpb.KeyValue{Key: key, Value: encoded})
	}
	return out, eventID, nil
}

func encodeEvents(events []sdktrace.Event) ([]*tracepb.Span_Event, error) {
	out := make([]*tracepb.Span_Event, 0, len(events))
	for _, event := range events {
		if event.DroppedAttributeCount != 0 {
			return nil, refuse("dropped-event-attributes", event.Name)
		}
		attributes, err := encodeEventAttributes(event.Attributes)
		if err != nil {
			return nil, err
		}
		when, ok := unixNano(event.Time)
		if !ok && !event.Time.IsZero() {
			return nil, refuse("span-event-time", event.Name)
		}
		out = append(out, &tracepb.Span_Event{
			TimeUnixNano: when,
			Name:         event.Name,
			Attributes:   attributes,
		})
	}
	return out, nil
}

func encodeEventAttributes(attributes []attribute.KeyValue) ([]*commonpb.KeyValue, error) {
	seen := make(map[string]struct{}, len(attributes))
	out := make([]*commonpb.KeyValue, 0, len(attributes))
	for _, kv := range attributes {
		key := string(kv.Key)
		if _, ok := seen[key]; ok {
			return nil, refuse("duplicate-attribute", key)
		}
		seen[key] = struct{}{}
		if isSentinel(key) {
			return nil, refuse("unknown-sentinel-attribute", key)
		}
		encoded, err := convertValue(key, kv.Value)
		if err != nil {
			return nil, err
		}
		if encoded == nil {
			continue
		}
		out = append(out, &commonpb.KeyValue{Key: key, Value: encoded})
	}
	return out, nil
}

func encodeResource(res *resource.Resource) ([]*commonpb.KeyValue, string, error) {
	if res == nil {
		return nil, "", nil
	}
	encoded, err := encodeCarried(res.Attributes())
	if err != nil {
		return nil, "", err
	}
	return encoded, res.SchemaURL(), nil
}

func encodeScope(scope instrumentation.Scope) (*commonpb.InstrumentationScope, error) {
	attributes, err := encodeCarried(scope.Attributes.ToSlice())
	if err != nil {
		return nil, err
	}
	if scope.Name == "" && scope.Version == "" && len(attributes) == 0 {
		return nil, nil
	}
	return &commonpb.InstrumentationScope{
		Name:       scope.Name,
		Version:    scope.Version,
		Attributes: attributes,
	}, nil
}

func encodeCarried(attributes []attribute.KeyValue) ([]*commonpb.KeyValue, error) {
	seen := make(map[string]struct{}, len(attributes))
	out := make([]*commonpb.KeyValue, 0, len(attributes))
	for _, kv := range attributes {
		key := string(kv.Key)
		if _, ok := seen[key]; ok {
			return nil, refuse("duplicate-attribute", key)
		}
		seen[key] = struct{}{}
		encoded, err := convertValue(key, kv.Value)
		if err != nil {
			return nil, err
		}
		if encoded == nil {
			continue
		}
		out = append(out, &commonpb.KeyValue{Key: key, Value: encoded})
	}
	return out, nil
}

func convertValue(key string, value attribute.Value) (*commonpb.AnyValue, error) {
	switch value.Type() {
	case attribute.EMPTY:
		return nil, nil
	case attribute.STRING:
		return stringAny(value.AsString()), nil
	case attribute.BOOL:
		return &commonpb.AnyValue{Value: &commonpb.AnyValue_BoolValue{BoolValue: value.AsBool()}}, nil
	case attribute.INT64:
		return &commonpb.AnyValue{Value: &commonpb.AnyValue_IntValue{IntValue: value.AsInt64()}}, nil
	case attribute.FLOAT64:
		return floatAny(key, value.AsFloat64())
	case attribute.BYTESLICE:
		return &commonpb.AnyValue{Value: &commonpb.AnyValue_BytesValue{BytesValue: append([]byte(nil), value.AsByteSlice()...)}}, nil
	case attribute.BOOLSLICE:
		return sliceAny(key, value.AsBoolSlice(), func(v bool) (attribute.Value, error) {
			return attribute.BoolValue(v), nil
		})
	case attribute.INT64SLICE:
		return sliceAny(key, value.AsInt64Slice(), func(v int64) (attribute.Value, error) {
			return attribute.Int64Value(v), nil
		})
	case attribute.FLOAT64SLICE:
		return sliceAny(key, value.AsFloat64Slice(), func(v float64) (attribute.Value, error) {
			return attribute.Float64Value(v), nil
		})
	case attribute.STRINGSLICE:
		return sliceAny(key, value.AsStringSlice(), func(v string) (attribute.Value, error) {
			return attribute.StringValue(v), nil
		})
	case attribute.SLICE:
		items := value.AsSlice()
		values := make([]*commonpb.AnyValue, 0, len(items))
		for _, item := range items {
			encoded, err := convertValue(key, item)
			if err != nil {
				return nil, err
			}
			if encoded == nil {
				continue
			}
			values = append(values, encoded)
		}
		return &commonpb.AnyValue{Value: &commonpb.AnyValue_ArrayValue{ArrayValue: &commonpb.ArrayValue{Values: values}}}, nil
	case attribute.MAP:
		items := value.AsMap()
		entries := make([]*commonpb.KeyValue, 0, len(items))
		for _, item := range items {
			nested := string(item.Key)
			if isSentinel(nested) {
				return nil, refuse("unknown-sentinel-attribute", nested)
			}
			encoded, err := convertValue(nested, item.Value)
			if err != nil {
				return nil, err
			}
			if encoded == nil {
				continue
			}
			entries = append(entries, &commonpb.KeyValue{Key: nested, Value: encoded})
		}
		return &commonpb.AnyValue{Value: &commonpb.AnyValue_KvlistValue{KvlistValue: &commonpb.KeyValueList{Values: entries}}}, nil
	default:
		return nil, refuse("unsupported-attribute", key)
	}
}

func sliceAny[T any](key string, items []T, convert func(T) (attribute.Value, error)) (*commonpb.AnyValue, error) {
	values := make([]*commonpb.AnyValue, 0, len(items))
	for _, item := range items {
		next, err := convert(item)
		if err != nil {
			return nil, err
		}
		encoded, err := convertValue(key, next)
		if err != nil {
			return nil, err
		}
		if encoded == nil {
			continue
		}
		values = append(values, encoded)
	}
	return &commonpb.AnyValue{Value: &commonpb.AnyValue_ArrayValue{ArrayValue: &commonpb.ArrayValue{Values: values}}}, nil
}

func floatAny(key string, number float64) (*commonpb.AnyValue, error) {
	if math.IsNaN(number) || math.IsInf(number, 0) {
		return nil, refuse("non-finite-double", key)
	}
	return &commonpb.AnyValue{Value: &commonpb.AnyValue_DoubleValue{DoubleValue: number}}, nil
}

func stringAny(text string) *commonpb.AnyValue {
	return &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: text}}
}

func requireString(key string, value attribute.Value) (string, error) {
	if value.Type() != attribute.STRING {
		return "", refuse("invalid-sentinel-attribute", key)
	}
	return value.AsString(), nil
}

func requireUint(key string, value attribute.Value) (uint64, error) {
	if value.Type() != attribute.INT64 {
		return 0, refuse("invalid-sentinel-attribute", key)
	}
	number := value.AsInt64()
	if number < 0 {
		return 0, refuse("invalid-sentinel-attribute", key)
	}
	return uint64(number), nil
}

func isSentinel(key string) bool {
	return len(key) >= len("sentinel.") && key[:len("sentinel.")] == "sentinel."
}

func onlySpace(text string) bool {
	for _, r := range text {
		if r != ' ' && r != '\t' && r != '\n' && r != '\r' {
			return false
		}
	}
	return true
}

func unixNano(t time.Time) (uint64, bool) {
	if t.IsZero() {
		return 0, false
	}
	nano := t.UnixNano()
	if nano < 0 {
		return 0, false
	}
	return uint64(nano), true
}

func spanKind(kind trace.SpanKind) tracepb.Span_SpanKind {
	switch kind {
	case trace.SpanKindInternal:
		return tracepb.Span_SPAN_KIND_INTERNAL
	case trace.SpanKindServer:
		return tracepb.Span_SPAN_KIND_SERVER
	case trace.SpanKindClient:
		return tracepb.Span_SPAN_KIND_CLIENT
	case trace.SpanKindProducer:
		return tracepb.Span_SPAN_KIND_PRODUCER
	case trace.SpanKindConsumer:
		return tracepb.Span_SPAN_KIND_CONSUMER
	default:
		return tracepb.Span_SPAN_KIND_UNSPECIFIED
	}
}

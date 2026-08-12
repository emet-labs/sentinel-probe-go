package emission

import (
	"go.opentelemetry.io/otel/attribute"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"

	modelv1 "github.com/emet-labs/sentinel/sdk/go/gen/sentinel/model/v1"
	"github.com/emet-labs/sentinel/sdk/go/int128"
)

// Reserved attribute keys. They carry causal-edge metadata and are never emitted as
// ProducerEvent attributes. Same string constants as the TypeScript reference.
const (
	AttributeEventID       = "sentinel.event.id"
	AttributeParentEventID = "sentinel.parent.event.id"
)

// Malformed-link sources reported through SpanConversion.OnMalformedLink.
const (
	MalformedLinkSourceParent = "parent"
	MalformedLinkSourceLink   = "link"
)

// SpanConversion is everything SpanToEvent needs beyond the span itself.
//
// Named SpanConversion rather than SpanContext (the TypeScript name) because
// trace.SpanContext appears in this very conversion — ReadOnlySpan.Parent() returns one and
// every Link carries one — and shadowing it would be actively confusing. For the same reason
// no parameter in this package is named ctx: in Go that means context.Context, which the
// enforcement gate genuinely takes.
type SpanConversion struct {
	// Span is the ended span to convert. In tests, build it with
	// tracetest.SpanStub{...}.Snapshot(); ReadOnlySpan is sealed and cannot be implemented.
	Span sdktrace.ReadOnlySpan
	// Sequence is the host-assigned sequence coordinate. Sequence assignment is out of SDK
	// scope: Probes are thin (ADR-0006).
	Sequence *modelv1.SequenceCoordinate
	// SchemaVersion of the emitted event.
	SchemaVersion string
	// AcknowledgedEpoch is the filter epoch this Probe has acknowledged, typically
	// ProbeClient.AcknowledgedEpoch(). nil means none is held; it does not mean 0.
	AcknowledgedEpoch *uint64
	// ClaimedCapabilities the source asserts. Claims only: the receiver owns effective
	// capabilities, tier, integrity and observation time by construction (event.proto:32-33).
	ClaimedCapabilities []modelv1.SourceCapability
	// ClaimedSensitivity of the event.
	ClaimedSensitivity modelv1.Sensitivity
	// EventID is the reserved sentinel.event.id the producer assigned at span start.
	EventID string
	// OnMalformedLink, when non-nil, is called once per causal predecessor that had to fall
	// back to a hex span ID because the producer contract was violated. source is
	// MalformedLinkSourceParent or MalformedLinkSourceLink.
	OnMalformedLink func(source, spanID string)
}

// SpanToEvent converts an ended OTel span into a ProducerEvent.
//
// The event's kind is the span name, its occurrence time is the span's start time, its
// attributes are the span's attributes minus the two reserved keys, and its causal
// predecessors come from the parent and link attributes described in the package doc.
func SpanToEvent(conversion SpanConversion) *modelv1.ProducerEvent {
	span := conversion.Span

	return &modelv1.ProducerEvent{
		Id:                      conversion.EventID,
		Sequence:                conversion.Sequence,
		SchemaVersion:           conversion.SchemaVersion,
		AcknowledgedFilterEpoch: conversion.AcknowledgedEpoch,
		Kind:                    span.Name(),
		OccurrenceTime:          BuildOccurrenceTime(span),
		Attributes:              mapAttributes(span.Attributes()),
		ClaimedCapabilities:     conversion.ClaimedCapabilities,
		ClaimedSensitivity:      conversion.ClaimedSensitivity,
		CausalPredecessorIds:    collectCausalPredecessors(span, conversion.OnMalformedLink),
	}
}

// BuildOccurrenceTime converts a span's start time into an OccurrenceTime in the "unix" clock
// domain.
//
// uncertainty_nanoseconds is always 0, mirroring span-to-event.ts:97-103 exactly.
//
// TODO(#33): SOURCE_CAPABILITY_BOUNDED_CLOCK_UNCERTAINTY has no Probe-side input today and
// SpanConversion deliberately carries no field to supply one, so there is no branch to
// write. testdata/sagashop/README.md documents why the Adapter withholds
// BoundedClockUncertainty for recorded captures; do not invent a Probe-side story that
// contradicts it.
func BuildOccurrenceTime(span sdktrace.ReadOnlySpan) *modelv1.OccurrenceTime {
	return &modelv1.OccurrenceTime{
		ClockDomainId:          "unix",
		Nanoseconds:            int128.FromBigInt(int128.TimeToNanoseconds(span.StartTime())),
		UncertaintyNanoseconds: 0,
	}
}

func mapAttributes(attributes []attribute.KeyValue) []*modelv1.AttributeEntry {
	entries := make([]*modelv1.AttributeEntry, 0, len(attributes))
	for _, kv := range attributes {
		key := string(kv.Key)
		if key == AttributeEventID || key == AttributeParentEventID {
			continue // reserved: causal-edge metadata, not a domain attribute
		}
		value := mapValue(kv.Value)
		if value == nil {
			continue
		}
		entries = append(entries, &modelv1.AttributeEntry{Key: key, Value: value})
	}
	return entries
}

// mapValue maps one attribute.Value onto the AttributeValue oneof. It returns nil for EMPTY,
// mirroring the reference skipping null and undefined.
func mapValue(value attribute.Value) *modelv1.AttributeValue {
	switch value.Type() {
	case attribute.STRING:
		return &modelv1.AttributeValue{Value: &modelv1.AttributeValue_StringValue{StringValue: value.AsString()}}
	case attribute.BOOL:
		return &modelv1.AttributeValue{Value: &modelv1.AttributeValue_BoolValue{BoolValue: value.AsBool()}}
	case attribute.INT64:
		return &modelv1.AttributeValue{Value: &modelv1.AttributeValue_IntegerValue{IntegerValue: value.AsInt64()}}
	case attribute.FLOAT64:
		// D14: type-directed, so float64(3) stays a double. TypeScript is value-directed and
		// would emit integer_value here.
		return &modelv1.AttributeValue{Value: &modelv1.AttributeValue_DoubleValue{DoubleValue: value.AsFloat64()}}
	case attribute.BYTESLICE:
		return &modelv1.AttributeValue{Value: &modelv1.AttributeValue_BytesValue{BytesValue: value.AsByteSlice()}}
	case attribute.BOOLSLICE:
		items := value.AsBoolSlice()
		values := make([]*modelv1.AttributeValue, 0, len(items))
		for _, item := range items {
			values = append(values, mapValue(attribute.BoolValue(item)))
		}
		return arrayValue(values)
	case attribute.INT64SLICE:
		items := value.AsInt64Slice()
		values := make([]*modelv1.AttributeValue, 0, len(items))
		for _, item := range items {
			values = append(values, mapValue(attribute.Int64Value(item)))
		}
		return arrayValue(values)
	case attribute.FLOAT64SLICE:
		items := value.AsFloat64Slice()
		values := make([]*modelv1.AttributeValue, 0, len(items))
		for _, item := range items {
			values = append(values, mapValue(attribute.Float64Value(item)))
		}
		return arrayValue(values)
	case attribute.STRINGSLICE:
		items := value.AsStringSlice()
		values := make([]*modelv1.AttributeValue, 0, len(items))
		for _, item := range items {
			values = append(values, mapValue(attribute.StringValue(item)))
		}
		return arrayValue(values)
	case attribute.SLICE:
		items := value.AsSlice()
		values := make([]*modelv1.AttributeValue, 0, len(items))
		for _, item := range items {
			mapped := mapValue(item)
			if mapped == nil {
				continue // skip empties, as the reference skips null members
			}
			values = append(values, mapped)
		}
		return arrayValue(values)
	case attribute.MAP:
		items := value.AsMap()
		entries := make([]*modelv1.AttributeEntry, 0, len(items))
		for _, kv := range items {
			mapped := mapValue(kv.Value)
			if mapped == nil {
				continue
			}
			entries = append(entries, &modelv1.AttributeEntry{Key: string(kv.Key), Value: mapped})
		}
		return &modelv1.AttributeValue{
			Value: &modelv1.AttributeValue_MapValue{MapValue: &modelv1.AttributeMap{Entries: entries}},
		}
	case attribute.EMPTY:
		return nil
	default:
		// Unreachable at otel v1.45.0; a future attribute type is skipped rather than
		// guessed at, because inventing a mapping would put data in the wrong oneof arm.
		return nil
	}
}

func arrayValue(values []*modelv1.AttributeValue) *modelv1.AttributeValue {
	return &modelv1.AttributeValue{
		Value: &modelv1.AttributeValue_ArrayValue{ArrayValue: &modelv1.AttributeArray{Values: values}},
	}
}

// collectCausalPredecessors reads the parent edge and every link edge, in that order.
func collectCausalPredecessors(span sdktrace.ReadOnlySpan, onMalformedLink func(source, spanID string)) []string {
	var predecessors []string

	// Parent: the child span carries the parent's event ID under the reserved key.
	if parentEventID, ok := stringAttribute(span.Attributes(), AttributeParentEventID); ok {
		predecessors = append(predecessors, parentEventID)
	} else if parent := span.Parent(); parent.IsValid() {
		// Parent() returns a VALUE, so validity is IsValid(), not a nil check.
		spanID := parent.SpanID().String()
		predecessors = append(predecessors, spanID)
		if onMalformedLink != nil {
			onMalformedLink(MalformedLinkSourceParent, spanID)
		}
	}

	// Links: each link carries its target's event ID in that link's OWN attributes.
	for _, link := range span.Links() {
		if linkEventID, ok := stringAttribute(link.Attributes, AttributeEventID); ok {
			predecessors = append(predecessors, linkEventID)
			continue
		}
		spanID := link.SpanContext.SpanID().String()
		predecessors = append(predecessors, spanID)
		if onMalformedLink != nil {
			onMalformedLink(MalformedLinkSourceLink, spanID)
		}
	}

	return predecessors
}

// stringAttribute returns the value of key when it is present AND holds a string. A non-string
// value under a reserved key is treated as absent, matching the reference's
// `typeof x === "string"` guard, so a mistyped attribute takes the malformed-link path rather
// than silently producing a garbage event ID.
func stringAttribute(attributes []attribute.KeyValue, key string) (string, bool) {
	for _, kv := range attributes {
		if string(kv.Key) != key {
			continue
		}
		if kv.Value.Type() != attribute.STRING {
			return "", false
		}
		return kv.Value.AsString(), true
	}
	return "", false
}

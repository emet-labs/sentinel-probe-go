// Package emission converts OTel Go spans into sentinel.model.v1.ProducerEvent values.
// Go analog of sdk/typescript/src/emission/ (span-to-event.ts, otel-bridge.ts).
//
// The SDK does not own export. ADR-0002 makes OTel an adapter, not the substrate: this
// package produces spans and converts them, and the host wires the OTLP exporter, exactly as
// SagaShop's Go port does in internal/otelx/tracing.go.
//
// # Producer contract for causal edges
//
// ProducerEvent.causal_predecessor_ids carries event IDs, not span IDs, so the producer must
// thread them through span attributes:
//
//   - at span start, the producer assigns an event ID and sets it as the span attribute
//     "sentinel.event.id";
//   - when linking to a predecessor span A, the producer stamps A's event ID into THAT
//     LINK'S OWN attributes under "sentinel.event.id";
//   - for the parent, the producer threads the parent's event ID into the child span's
//     attributes under "sentinel.parent.event.id".
//
// SpanToEvent reads predecessors from exactly those places. It falls back to the hex span ID
// only when the attribute is absent, and reports every such fallback through
// SpanConversion.OnMalformedLink, because a fallback means the producer contract was
// violated and the resulting edge is not joinable with anything else. Both reserved keys are
// excluded from ProducerEvent.attributes: they carry causal-edge metadata, not domain data.
//
// # Constructing spans in tests
//
// sdktrace.ReadOnlySpan is a SEALED interface — it declares an unexported private() method,
// so it cannot be implemented outside the OTel SDK package. The TypeScript reference fakes
// its ReadableSpan directly because that is an ordinary interface; Go has exactly one
// construction path, go.opentelemetry.io/otel/sdk/trace/tracetest:
//
//	stub := tracetest.SpanStub{Name: ..., Parent: ..., Attributes: ..., Links: ...}
//	event := emission.SpanToEvent(emission.SpanConversion{Span: stub.Snapshot(), ...})
//
// tracetest ships inside the otel/sdk module that is already required, so this costs nothing.
// Note that ReadOnlySpan.Parent() returns a trace.SpanContext VALUE, not a pointer: the
// TypeScript truthiness check becomes span.Parent().IsValid(), and a nil check does not
// compile.
//
// # Attribute mapping
//
// At otel v1.45.0 the attribute.Type set is EMPTY, BOOL, INT64, FLOAT64, STRING, BOOLSLICE,
// INT64SLICE, FLOAT64SLICE, STRINGSLICE, BYTESLICE, SLICE and MAP, so all seven
// AttributeValue oneof arms in event.proto are reachable natively:
//
//	STRING                                     -> string_value
//	BOOL                                       -> bool_value
//	INT64                                      -> integer_value
//	FLOAT64                                    -> double_value
//	BYTESLICE                                  -> bytes_value
//	BOOLSLICE/INT64SLICE/FLOAT64SLICE/STRINGSLICE -> array_value (homogeneous)
//	SLICE ([]Value)                            -> array_value (heterogeneous, recursive)
//	MAP ([]KeyValue)                           -> map_value (recursive)
//	EMPTY                                      -> skipped, mirroring TypeScript skipping null
//
// Earlier otel releases had no bytes, heterogeneous-slice or map type, which would have
// forced an escape hatch for the host to supply those arms directly and would have left Go's
// array_value homogeneous-only where TypeScript's recurses. Pinning v1.45.0 removes that
// cross-SDK asymmetry entirely.
//
// Divergence D14: integer/double dispatch is TYPE-directed here and VALUE-directed in
// TypeScript. span-to-event.ts branches on Number.isInteger(value), so JavaScript 3 and 3.0
// are the same value and both become integer_value. Go dispatches on attribute.INT64 versus
// attribute.FLOAT64, so float64(3) stays a double_value. Go's behaviour is the correct one
// and is not a bug to be reported as drift.
//
// A span's own attributes are an ordered []attribute.KeyValue, not a JavaScript object, so
// the emitted attribute order is the span's own order and the order assertions in the tests
// are stronger than the reference's. Members of a MAP value are the exception: attribute.Map
// canonicalises their key order, so map_value entries are compared by key, not by position.
package emission

package emission_test

import (
	"context"
	"slices"
	"testing"
	"time"

	"go.opentelemetry.io/otel/attribute"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"

	"github.com/emet-labs/sentinel/sdk/go/emission"
	"github.com/emet-labs/sentinel/sdk/go/enforcement"
	"github.com/emet-labs/sentinel/sdk/go/filter"
	modelv1 "github.com/emet-labs/sentinel/sdk/go/gen/sentinel/model/v1"
	probev1 "github.com/emet-labs/sentinel/sdk/go/gen/sentinel/probe/v1"
)

// An end-to-end emit -> ApplyFilter -> Gate exercise over realistically shaped spans.
//
// The span names, attribute keys and attribute values below are lifted by hand from
// testdata/sagashop/ts-happy.json (`order.charge`, `POST /orders/{id}/charge`,
// `sagashop.order.id`, `sagashop.order.state`, `http.route`, `http.status_code`, and the
// captured order and charge identifiers), so the test runs over the shape of real traffic
// rather than over `kind.a` and `key.b`.
//
// It deliberately does NOT decode the capture envelope. That was the original proposal and it
// was cut for four reasons, all of which hold:
//
//   - every attribute key in those captures is http.* or sagashop.*, with ZERO
//     sentinel.event.id / sentinel.parent.event.id, so every span would take the
//     malformed-link fallback and the test would exercise the producer contract VIOLATION
//     path rather than the contract;
//   - the envelope carries no OTel spans, so each one would have to be rebuilt as a
//     tracetest.SpanStub anyway;
//   - Binding, the concept the ADR-0023 flagship property is stated over, exists only in
//     core/, model/, export/ and ingest/ — never in a Probe — so "a mock decider implementing
//     the flagship property" is a hardcoded DENY, and the assertion would BE the fixture;
//   - ingest/src/capture.rs and ingest/src/sagashop.rs already own decoding that byte-frozen
//     artifact, and a second Go decoder with no drift gate and no link to the provenance and
//     SHA-256 block in testdata/sagashop/README.md is a liability, not evidence.
//
// The spans here are therefore wired WITH the reserved sentinel.event.id,
// sentinel.parent.event.id and link attributes the producer contract requires, so the causal
// edges assert the contract rather than the fallback.
//
// This is not the integration test #33's fifth acceptance criterion asks for, and it is not
// presented as one: no SentinelDecisionService is served anywhere in this repository. See
// sdk/go/README.md and the PR body.

const (
	orderID          = "779d1470-d075-4e60-ada4-3c2a72064df1"
	chargeID         = "adbecc31-e9ba-45a2-a98b-4c32b960952b"
	chargeRoute      = `^\/orders\/(?<id>[^/]+)\/charge$`
	driverEventID    = "3f6e2c7a-0c2f-4a3f-8e10-1d1a3d2c9b01"
	chargeEventID    = "8a1d5f2b-6b74-4d6f-9d0e-2b7c4e5a1f22"
	reserveEventID   = "c04b9e17-4b0a-4a52-9a3f-77c1d2e3f4a5"
	chargeSpanKind   = "POST /orders/" + orderID + "/charge"
	realisticEpoch   = uint64(11)
	realisticHandle  = "sagashop.order"
	realisticVersion = "sentinel.model.v1"
)

// chargeSpan is the SagaShop order service's charge handler, with a parent edge to the
// driver's order.charge span and a link to the inventory reservation that must causally
// precede it.
func chargeSpan(t *testing.T) sdktrace.ReadOnlySpan {
	t.Helper()
	return snapshot(tracetest.SpanStub{
		Name:      chargeSpanKind,
		StartTime: time.Unix(1700000020, 0),
		Parent:    spanContext(t, "13912439bfec6d828fe6726a5ce24aed", "64f2006c5ca59afd"),
		Attributes: []attribute.KeyValue{
			attribute.String(emission.AttributeEventID, chargeEventID),
			attribute.String(emission.AttributeParentEventID, driverEventID),
			attribute.String("http.method", "POST"),
			attribute.String("http.route", chargeRoute),
			attribute.Int64("http.status_code", 200),
			attribute.String("sagashop.event", "order.charge"),
			attribute.String("sagashop.order.id", orderID),
			attribute.String("sagashop.order.prev_state", "RESERVED"),
			attribute.String("sagashop.order.state", "PAID"),
			attribute.String("sagashop.payment.charge_id", chargeID),
			attribute.String("sagashop.payment.status", "success"),
		},
		Links: []sdktrace.Link{{
			SpanContext: spanContext(t, "13912439bfec6d828fe6726a5ce24aed", "3b79b7f1b137205f"),
			Attributes:  []attribute.KeyValue{attribute.String(emission.AttributeEventID, reserveEventID)},
		}},
	})
}

// chargeFilter enforces the charge event and projects only the attributes a Specification
// about order state transitions could need.
func chargeFilter() *modelv1.EventFilter {
	epoch := realisticEpoch
	return &modelv1.EventFilter{
		Epoch: &epoch,
		Specifications: []*modelv1.SpecificationFilter{{
			SpecificationId:      "order-state-transition",
			SpecificationVersion: "1.0.0",
			EventMatch: &modelv1.EventMatch{
				EventKinds: []string{chargeSpanKind},
				ProjectedAttributeKeys: []string{
					"sagashop.order.id", "sagashop.order.prev_state", "sagashop.order.state",
				},
				DeliveryMode: modelv1.DeliveryMode_DELIVERY_MODE_ASK_AND_BLOCK,
			},
			FailMode: modelv1.FailMode_FAIL_MODE_CLOSED,
		}},
	}
}

func realisticEvent(t *testing.T) *modelv1.ProducerEvent {
	t.Helper()
	epoch := realisticEpoch
	var malformed []string
	event := emission.SpanToEvent(emission.SpanConversion{
		Span:              chargeSpan(t),
		Sequence:          &modelv1.SequenceCoordinate{Epoch: 1, Sequence: 5},
		SchemaVersion:     realisticVersion,
		AcknowledgedEpoch: &epoch,
		ClaimedCapabilities: []modelv1.SourceCapability{
			modelv1.SourceCapability_SOURCE_CAPABILITY_CAUSAL_EDGES,
		},
		ClaimedSensitivity: modelv1.Sensitivity_SENSITIVITY_INTERNAL,
		EventID:            chargeEventID,
		OnMalformedLink:    func(source, _ string) { malformed = append(malformed, source) },
	})
	if len(malformed) != 0 {
		t.Fatalf("a contract-conformant producer must report no malformed links, got %v", malformed)
	}
	return event
}

func TestRealisticShapeEmitProjectAndPermit(t *testing.T) {
	t.Parallel()

	event := realisticEvent(t)

	// Emission: the span name is the event kind, the reserved keys are excluded, and the
	// causal edges are event IDs — the parent from the child's own reserved attribute, the
	// predecessor from that link's own attributes.
	if event.GetKind() != chargeSpanKind {
		t.Fatalf("Kind = %q", event.GetKind())
	}
	if event.GetId() != chargeEventID {
		t.Fatalf("Id = %q", event.GetId())
	}
	wantEdges := []string{driverEventID, reserveEventID}
	if !slices.Equal(event.GetCausalPredecessorIds(), wantEdges) {
		t.Fatalf("causal predecessors = %v, want %v", event.GetCausalPredecessorIds(), wantEdges)
	}
	if len(event.GetAttributes()) != 9 {
		t.Fatalf("emitted %d attributes, want the 9 domain attributes (both reserved keys excluded)",
			len(event.GetAttributes()))
	}

	// Projection: only the three keys the Specification declared survive, and the causal
	// skeleton is untouched.
	projected := filter.ApplyFilter(event, chargeFilter())
	if projected == nil {
		t.Fatal("an enforcing spec selects this kind, so the event must not be dropped")
	}
	var keys []string
	for _, entry := range projected.GetAttributes() {
		keys = append(keys, entry.GetKey())
	}
	want := []string{"sagashop.order.id", "sagashop.order.prev_state", "sagashop.order.state"}
	if !slices.Equal(keys, want) {
		t.Fatalf("projected keys = %v, want %v", keys, want)
	}
	if !slices.Equal(projected.GetCausalPredecessorIds(), wantEdges) {
		t.Fatal("causal predecessors must never be trimmed by projection")
	}
	if projected.GetOccurrenceTime().GetClockDomainId() != "unix" {
		t.Fatal("occurrence time must survive projection")
	}

	// Enforcement: the endpoint permits, so the Probe proceeds.
	mock := &realisticDecider{action: probev1.DecisionAction_DECISION_ACTION_PERMIT}
	outcome := enforcement.Gate(t.Context(), projected, chargeFilter(), nil, enforcement.Deps{
		Decide:         mock.decide,
		NowMonotonicNs: func() int64 { return 0 },
		AcceptedFailModeFor: func(*modelv1.SpecificationFilter) modelv1.FailMode {
			return modelv1.FailMode_FAIL_MODE_CLOSED
		},
	}, enforcement.Options{SourceHandle: realisticHandle, RequestID: "req-1", IdempotencyKey: "idem-1"})

	if outcome.Kind != enforcement.OutcomePermit {
		t.Fatalf("Kind = %v, want permit", outcome.Kind)
	}
	if mock.request.GetProducerEvent().GetKind() != chargeSpanKind {
		t.Fatalf("the projected event must reach the endpoint: %v", mock.request.GetProducerEvent())
	}
	if len(mock.request.GetProducerEvent().GetAttributes()) != 3 {
		t.Fatalf("the endpoint must receive the PROJECTED event, got %d attributes",
			len(mock.request.GetProducerEvent().GetAttributes()))
	}
	if mock.request.FilterEpoch == nil || *mock.request.FilterEpoch != realisticEpoch {
		t.Fatalf("FilterEpoch = %v", mock.request.FilterEpoch)
	}
}

// TestRealisticShapeUnreachableEndpointFailsClosed: the same flow, but the endpoint is
// unreachable and the deployment has contracted fail-closed for this Specification, so the
// charge is blocked rather than waved through.
func TestRealisticShapeUnreachableEndpointFailsClosed(t *testing.T) {
	t.Parallel()

	projected := filter.ApplyFilter(realisticEvent(t), chargeFilter())
	mock := &realisticDecider{err: context.DeadlineExceeded}

	outcome := enforcement.Gate(t.Context(), projected, chargeFilter(), nil, enforcement.Deps{
		Decide:         mock.decide,
		NowMonotonicNs: func() int64 { return 0 },
		AcceptedFailModeFor: func(*modelv1.SpecificationFilter) modelv1.FailMode {
			return modelv1.FailMode_FAIL_MODE_CLOSED
		},
	}, enforcement.Options{SourceHandle: realisticHandle, RequestID: "req-1", IdempotencyKey: "idem-1"})

	if outcome.Kind != enforcement.OutcomeFailClosedDeny {
		t.Fatalf("Kind = %v, want fail-closed-deny", outcome.Kind)
	}
	if outcome.FilterEpoch == nil || *outcome.FilterEpoch != realisticEpoch {
		t.Fatalf("FilterEpoch = %v, want the epoch audited on the fail path too", outcome.FilterEpoch)
	}
}

// TestRealisticShapeUnreachableEndpointFailsOpenWithoutAContract: identical, except no
// fail-closed contract has been accepted, so the declaration alone downgrades to fail-open.
func TestRealisticShapeUnreachableEndpointFailsOpenWithoutAContract(t *testing.T) {
	t.Parallel()

	projected := filter.ApplyFilter(realisticEvent(t), chargeFilter())
	mock := &realisticDecider{err: context.DeadlineExceeded}

	outcome := enforcement.Gate(t.Context(), projected, chargeFilter(), nil, enforcement.Deps{
		Decide:         mock.decide,
		NowMonotonicNs: func() int64 { return 0 },
		AcceptedFailModeFor: func(*modelv1.SpecificationFilter) modelv1.FailMode {
			return modelv1.FailMode_FAIL_MODE_OPEN
		},
	}, enforcement.Options{SourceHandle: realisticHandle, RequestID: "req-1", IdempotencyKey: "idem-1"})

	if outcome.Kind != enforcement.OutcomeFailOpenPermit {
		t.Fatalf("Kind = %v, want fail-open-permit", outcome.Kind)
	}
}

// TestRealisticShapeIrrelevantEventIsDropped: a span from the same trace that no Specification
// selects is dropped by projection, which is relevance, not sampling.
func TestRealisticShapeIrrelevantEventIsDropped(t *testing.T) {
	t.Parallel()

	reserve := emission.SpanToEvent(emission.SpanConversion{
		Span: snapshot(tracetest.SpanStub{
			Name:      "inventory.reserve",
			StartTime: time.Unix(1700000010, 0),
			Attributes: []attribute.KeyValue{
				attribute.String(emission.AttributeEventID, reserveEventID),
				attribute.String("http.method", "POST"),
				attribute.Int64("sagashop.inventory.qty", 1),
			},
		}),
		SchemaVersion: realisticVersion,
		EventID:       reserveEventID,
	})

	if projected := filter.ApplyFilter(reserve, chargeFilter()); projected != nil {
		t.Fatalf("no spec selects inventory.reserve, so it must be dropped, got %v", projected)
	}
}

type realisticDecider struct {
	action  probev1.DecisionAction
	err     error
	request *probev1.DecideRequest
}

func (d *realisticDecider) decide(
	_ context.Context,
	request *probev1.DecideRequest,
) (*probev1.DecideResponse, error) {
	d.request = request
	if d.err != nil {
		return nil, d.err
	}
	return &probev1.DecideResponse{RequestId: request.GetRequestId(), Action: d.action}, nil
}

package enforcement_test

import (
	"context"
	"errors"
	"strings"
	"testing"

	"connectrpc.com/connect"

	"github.com/emet-labs/sentinel/sdk/go/enforcement"
	modelv1 "github.com/emet-labs/sentinel/sdk/go/gen/sentinel/model/v1"
	probev1 "github.com/emet-labs/sentinel/sdk/go/gen/sentinel/probe/v1"
)

const (
	testKind  = "transfer.initiated"
	testEpoch = uint64(5)
)

var testOptions = enforcement.Options{
	SourceHandle:   "gateway.tool-calls",
	RequestID:      "req-1",
	IdempotencyKey: "idem-1",
}

func u64(v uint64) *uint64 { return &v }
func i64(v int64) *int64   { return &v }

func makeEvent(kind string) *modelv1.ProducerEvent {
	return &modelv1.ProducerEvent{Id: "evt-1", Kind: kind, SchemaVersion: "sentinel.model.v1"}
}

func makeSpec(
	specificationID string,
	kinds []string,
	failMode modelv1.FailMode,
	deliveryMode modelv1.DeliveryMode,
) *modelv1.SpecificationFilter {
	return &modelv1.SpecificationFilter{
		SpecificationId:      specificationID,
		SpecificationVersion: "1.0.0",
		EventMatch: &modelv1.EventMatch{
			EventKinds:   kinds,
			DeliveryMode: deliveryMode,
		},
		FailMode: failMode,
		EvaluationMode: modelv1.EvaluationMode_EVALUATION_MODE_ENFORCE,
		Readiness: modelv1.Readiness_READINESS_ACTIVE,
		LatencyBudgetNanoseconds: u64(10000),
	}
}

func askAndBlockSpec() *modelv1.SpecificationFilter {
	return makeSpec("spec-1", []string{testKind},
		modelv1.FailMode_FAIL_MODE_OPEN, modelv1.DeliveryMode_DELIVERY_MODE_ASK_AND_BLOCK)
}

func closedAskAndBlockSpec() *modelv1.SpecificationFilter {
	return makeSpec("spec-1", []string{testKind},
		modelv1.FailMode_FAIL_MODE_CLOSED, modelv1.DeliveryMode_DELIVERY_MODE_ASK_AND_BLOCK)
}

func shipAsyncSpec() *modelv1.SpecificationFilter {
	return makeSpec("spec-1", []string{testKind},
		modelv1.FailMode_FAIL_MODE_OPEN, modelv1.DeliveryMode_DELIVERY_MODE_SHIP_ASYNC)
}

func makeFilter(epoch *uint64, specs ...*modelv1.SpecificationFilter) *modelv1.EventFilter {
	return &modelv1.EventFilter{Epoch: epoch, Specifications: specs}
}

func makeDeps(
	mock *mockDecider,
	nowNs int64,
	accepted func(spec *modelv1.SpecificationFilter) modelv1.FailMode,
) enforcement.Deps {
	if accepted == nil {
		accepted = func(*modelv1.SpecificationFilter) modelv1.FailMode {
			return modelv1.FailMode_FAIL_MODE_OPEN
		}
	}
	return enforcement.Deps{
		Decide:              mock.decide,
		NowMonotonicNs:      func() int64 { return nowNs },
		AcceptedFailModeFor: accepted,
	}
}

func alwaysClosed(*modelv1.SpecificationFilter) modelv1.FailMode {
	return modelv1.FailMode_FAIL_MODE_CLOSED
}

func alwaysOpen(*modelv1.SpecificationFilter) modelv1.FailMode {
	return modelv1.FailMode_FAIL_MODE_OPEN
}

func TestGatePermit(t *testing.T) {
	t.Parallel()

	mock := &mockDecider{response: makeResponse(probev1.DecisionAction_DECISION_ACTION_PERMIT)}
	outcome := enforcement.Gate(t.Context(), makeEvent(testKind),
		makeFilter(u64(testEpoch), askAndBlockSpec()), i64(10000), makeDeps(mock, 0, nil), testOptions)

	if outcome.Kind != enforcement.OutcomePermit {
		t.Fatalf("Kind = %v, want permit", outcome.Kind)
	}
	if outcome.FilterEpoch == nil || *outcome.FilterEpoch != testEpoch {
		t.Fatalf("FilterEpoch = %v, want %d", outcome.FilterEpoch, testEpoch)
	}
	if !outcome.Kind.Permitted() {
		t.Fatal("permit must be permitted")
	}
}

func TestGateDeny(t *testing.T) {
	t.Parallel()

	mock := &mockDecider{response: makeResponse(probev1.DecisionAction_DECISION_ACTION_DENY)}
	outcome := enforcement.Gate(t.Context(), makeEvent(testKind),
		makeFilter(u64(testEpoch), askAndBlockSpec()), i64(10000), makeDeps(mock, 0, nil), testOptions)

	if outcome.Kind != enforcement.OutcomeDeny {
		t.Fatalf("Kind = %v, want deny", outcome.Kind)
	}
	if outcome.Kind.Permitted() {
		t.Fatal("deny must not be permitted")
	}
}

func TestGateDeferWithBudgetRemaining(t *testing.T) {
	t.Parallel()

	mock := &mockDecider{response: makeResponse(probev1.DecisionAction_DECISION_ACTION_DEFER)}
	outcome := enforcement.Gate(t.Context(), makeEvent(testKind),
		makeFilter(u64(testEpoch), askAndBlockSpec()), i64(10000), makeDeps(mock, 0, nil), testOptions)

	if outcome.Kind != enforcement.OutcomeDefer {
		t.Fatalf("Kind = %v, want defer", outcome.Kind)
	}
	if outcome.Kind.Permitted() {
		t.Fatal("defer is not a permit")
	}
}

func TestGateDeferBudgetExhaustedFailsOpen(t *testing.T) {
	t.Parallel()

	mock := &mockDecider{response: makeResponse(probev1.DecisionAction_DECISION_ACTION_DEFER)}
	// The clock advances between entry and response, so the post-response budget check sees 0.
	deps := makeDeps(mock, 0, nil)
	calls := 0
	deps.NowMonotonicNs = func() int64 {
		calls++
		if calls == 1 {
			return 0 // at entry: budget remains, so the ask happens
		}
		return 10000 // after the response: budget is gone
	}

	outcome := enforcement.Gate(t.Context(), makeEvent(testKind),
		makeFilter(u64(testEpoch), askAndBlockSpec()), i64(10000), deps, testOptions)

	if outcome.Kind != enforcement.OutcomeFailOpenPermit {
		t.Fatalf("Kind = %v, want fail-open-permit", outcome.Kind)
	}
	if outcome.Reason != "defer-budget-exhausted" {
		t.Fatalf("Reason = %q", outcome.Reason)
	}
	if mock.callCount() != 1 {
		t.Fatalf("callCount = %d, want 1 — the ask happened before the budget ran out", mock.callCount())
	}
}

func TestGateDeferBudgetExhaustedFailsClosedWhenContracted(t *testing.T) {
	t.Parallel()

	mock := &mockDecider{response: makeResponse(probev1.DecisionAction_DECISION_ACTION_DEFER)}
	deps := makeDeps(mock, 0, alwaysClosed)
	calls := 0
	deps.NowMonotonicNs = func() int64 {
		calls++
		if calls == 1 {
			return 0
		}
		return 10000
	}

	outcome := enforcement.Gate(t.Context(), makeEvent(testKind),
		makeFilter(u64(testEpoch), closedAskAndBlockSpec()), i64(10000), deps, testOptions)

	if outcome.Kind != enforcement.OutcomeFailClosedDeny {
		t.Fatalf("Kind = %v, want fail-closed-deny", outcome.Kind)
	}
}

func TestGateTransportErrorFailsOpenByDefault(t *testing.T) {
	t.Parallel()

	mock := &mockDecider{err: errors.New("connection refused")}
	outcome := enforcement.Gate(t.Context(), makeEvent(testKind),
		makeFilter(u64(testEpoch), askAndBlockSpec()), i64(10000), makeDeps(mock, 0, nil), testOptions)

	if outcome.Kind != enforcement.OutcomeFailOpenPermit {
		t.Fatalf("Kind = %v, want fail-open-permit", outcome.Kind)
	}
	if outcome.Reason != "transport-error: connection refused" {
		t.Fatalf("Reason = %q", outcome.Reason)
	}
}

func TestGateTransportErrorFailsClosedWhenContracted(t *testing.T) {
	t.Parallel()

	mock := &mockDecider{err: errors.New("connection refused")}
	outcome := enforcement.Gate(t.Context(), makeEvent(testKind),
		makeFilter(u64(testEpoch), closedAskAndBlockSpec()), i64(10000),
		makeDeps(mock, 0, alwaysClosed), testOptions)

	if outcome.Kind != enforcement.OutcomeFailClosedDeny {
		t.Fatalf("Kind = %v, want fail-closed-deny", outcome.Kind)
	}
}

// TestGateFailClosedRequiresAnAcceptedContract: declaring CLOSED is not enough. An operator
// has to have agreed to be blocked, otherwise the mode downgrades to OPEN.
func TestGateFailClosedRequiresAnAcceptedContract(t *testing.T) {
	t.Parallel()

	mock := &mockDecider{err: errors.New("connection refused")}
	outcome := enforcement.Gate(t.Context(), makeEvent(testKind),
		makeFilter(u64(testEpoch), closedAskAndBlockSpec()), i64(10000),
		makeDeps(mock, 0, nil), testOptions) // declared CLOSED, contract not accepted

	if outcome.Kind != enforcement.OutcomeFailOpenPermit {
		t.Fatalf("Kind = %v, want the downgrade to fail-open-permit", outcome.Kind)
	}
}

// TestGateNilAcceptedFailModeForPanics: a missing contract source is a wiring bug, not a
// default. Silently treating it as "nothing accepted" would make it the only dependency whose
// absence WEAKENS enforcement — a deployment that wires up Decide and forgets this one would
// get a gate that can never fail closed, with no signal at all. Nil Decide and nil
// NowMonotonicNs already panic; this makes the third consistent with them, and with the
// reference, which computes the aggregate fail mode outside its try/catch
// (enforcement-gate.ts:82, before the try at :108) so a missing implementation throws.
func TestGateNilAcceptedFailModeForPanics(t *testing.T) {
	t.Parallel()

	defer func() {
		recovered := recover()
		if recovered == nil {
			t.Fatal("a nil AcceptedFailModeFor must panic, not default to fail-open")
		}
		message, ok := recovered.(string)
		if !ok || !strings.Contains(message, "Deps.AcceptedFailModeFor is required") {
			t.Fatalf("panic value = %v, want it to name the missing dependency", recovered)
		}
	}()

	mock := &mockDecider{err: errors.New("connection refused")}
	deps := enforcement.Deps{Decide: mock.decide, NowMonotonicNs: func() int64 { return 0 }}
	enforcement.Gate(t.Context(), makeEvent(testKind),
		makeFilter(u64(testEpoch), closedAskAndBlockSpec()), i64(10000), deps, testOptions)
}

// TestGateWithoutEnforcingSpecsNeedsNoContractSource: the dependency is only required once an
// ask-and-block Specification selects the event, so a Probe whose filter is entirely
// ship-async never has to supply it, and the no-filter guard needs no dependencies at all.
func TestGateWithoutEnforcingSpecsNeedsNoContractSource(t *testing.T) {
	t.Parallel()

	mock := &mockDecider{}
	deps := enforcement.Deps{Decide: mock.decide, NowMonotonicNs: func() int64 { return 0 }}
	outcome := enforcement.Gate(t.Context(), makeEvent(testKind),
		makeFilter(u64(testEpoch), shipAsyncSpec()), i64(10000), deps, testOptions)
	if outcome.Kind != enforcement.OutcomePermit {
		t.Fatalf("Kind = %v, want permit without ever needing the contract source", outcome.Kind)
	}

	if got := enforcement.Gate(t.Context(), makeEvent(testKind), nil, i64(10000),
		enforcement.Deps{}, testOptions); got.Kind != enforcement.OutcomeNoFilter {
		t.Fatalf("Kind = %v, want no-filter with no dependencies at all", got.Kind)
	}
}

// TestGateAggregateFailClosedWins: one contracted-closed spec among many open ones decides
// the aggregate.
func TestGateAggregateFailClosedWins(t *testing.T) {
	t.Parallel()

	mock := &mockDecider{err: errors.New("connection refused")}
	openSpec := makeSpec("open-spec", []string{testKind},
		modelv1.FailMode_FAIL_MODE_OPEN, modelv1.DeliveryMode_DELIVERY_MODE_ASK_AND_BLOCK)
	closedSpec := makeSpec("closed-spec", []string{testKind},
		modelv1.FailMode_FAIL_MODE_CLOSED, modelv1.DeliveryMode_DELIVERY_MODE_ASK_AND_BLOCK)

	outcome := enforcement.Gate(t.Context(), makeEvent(testKind),
		makeFilter(u64(testEpoch), openSpec, closedSpec), i64(10000),
		makeDeps(mock, 0, func(spec *modelv1.SpecificationFilter) modelv1.FailMode {
			if spec.GetSpecificationId() == "closed-spec" {
				return modelv1.FailMode_FAIL_MODE_CLOSED
			}
			return modelv1.FailMode_FAIL_MODE_OPEN
		}), testOptions)

	if outcome.Kind != enforcement.OutcomeFailClosedDeny {
		t.Fatalf("Kind = %v, want fail-closed to win the aggregate", outcome.Kind)
	}
}

func TestGateUnspecifiedFailModeIsOpen(t *testing.T) {
	t.Parallel()

	mock := &mockDecider{err: errors.New("connection refused")}
	spec := makeSpec("spec-1", []string{testKind},
		modelv1.FailMode_FAIL_MODE_UNSPECIFIED, modelv1.DeliveryMode_DELIVERY_MODE_ASK_AND_BLOCK)
	outcome := enforcement.Gate(t.Context(), makeEvent(testKind),
		makeFilter(u64(testEpoch), spec), i64(10000), makeDeps(mock, 0, alwaysClosed), testOptions)

	if outcome.Kind != enforcement.OutcomeFailOpenPermit {
		t.Fatalf("Kind = %v: an undeclared fail mode is OPEN", outcome.Kind)
	}
}

func TestGateShipAsyncSpecsArePermittedWithoutAsking(t *testing.T) {
	t.Parallel()

	mock := &mockDecider{}
	outcome := enforcement.Gate(t.Context(), makeEvent(testKind),
		makeFilter(u64(testEpoch), shipAsyncSpec()), i64(10000), makeDeps(mock, 0, nil), testOptions)

	if outcome.Kind != enforcement.OutcomePermit {
		t.Fatalf("Kind = %v, want permit", outcome.Kind)
	}
	if mock.callCount() != 0 {
		t.Fatalf("callCount = %d, want 0 — ship-async never asks", mock.callCount())
	}
	if outcome.FilterEpoch == nil || *outcome.FilterEpoch != testEpoch {
		t.Fatalf("FilterEpoch = %v, want the epoch audited even on a no-op", outcome.FilterEpoch)
	}
}

func TestGateNonSelectingSpecIsNotEnforcing(t *testing.T) {
	t.Parallel()

	mock := &mockDecider{}
	other := makeSpec("spec-1", []string{"approval.granted"},
		modelv1.FailMode_FAIL_MODE_CLOSED, modelv1.DeliveryMode_DELIVERY_MODE_ASK_AND_BLOCK)
	outcome := enforcement.Gate(t.Context(), makeEvent(testKind),
		makeFilter(u64(testEpoch), other), i64(10000), makeDeps(mock, 0, alwaysClosed), testOptions)

	if outcome.Kind != enforcement.OutcomePermit || mock.callCount() != 0 {
		t.Fatalf("Kind = %v callCount = %d: a spec that does not select must not enforce",
			outcome.Kind, mock.callCount())
	}
}

// TestGateBudgetExhaustedSkipsDecide: the call count assertion is the point. A Probe already
// out of time must not spend more of it asking.
func TestGateBudgetExhaustedSkipsDecide(t *testing.T) {
	t.Parallel()

	mock := &mockDecider{response: makeResponse(probev1.DecisionAction_DECISION_ACTION_PERMIT)}
	outcome := enforcement.Gate(t.Context(), makeEvent(testKind),
		makeFilter(u64(testEpoch), askAndBlockSpec()), i64(10000), makeDeps(mock, 10000, nil), testOptions)

	if outcome.Kind != enforcement.OutcomeFailOpenPermit {
		t.Fatalf("Kind = %v, want fail-open-permit", outcome.Kind)
	}
	if outcome.Reason != "budget-exhausted" {
		t.Fatalf("Reason = %q", outcome.Reason)
	}
	if mock.callCount() != 0 {
		t.Fatalf("callCount = %d, want 0", mock.callCount())
	}
}

func TestGateBudgetAlreadyPastDeadlineSkipsDecide(t *testing.T) {
	t.Parallel()

	mock := &mockDecider{}
	outcome := enforcement.Gate(t.Context(), makeEvent(testKind),
		makeFilter(u64(testEpoch), askAndBlockSpec()), i64(10000), makeDeps(mock, 99999, nil), testOptions)

	if outcome.Kind != enforcement.OutcomeFailOpenPermit || mock.callCount() != 0 {
		t.Fatalf("a passed deadline must behave as an exhausted budget: %v, calls=%d",
			outcome.Kind, mock.callCount())
	}
}

func TestGateUnspecifiedActionIsNotABlindPermit(t *testing.T) {
	t.Parallel()

	mock := &mockDecider{response: makeResponse(probev1.DecisionAction_DECISION_ACTION_UNSPECIFIED)}
	outcome := enforcement.Gate(t.Context(), makeEvent(testKind),
		makeFilter(u64(testEpoch), askAndBlockSpec()), i64(10000), makeDeps(mock, 0, nil), testOptions)

	if outcome.Kind != enforcement.OutcomeFailOpenPermit {
		t.Fatalf("Kind = %v, want the fail mode rather than a permit", outcome.Kind)
	}
	if outcome.Reason != "unspecified-action" {
		t.Fatalf("Reason = %q", outcome.Reason)
	}
}

func TestGateUnspecifiedActionFailsClosedWhenContracted(t *testing.T) {
	t.Parallel()

	mock := &mockDecider{response: makeResponse(probev1.DecisionAction_DECISION_ACTION_UNSPECIFIED)}
	outcome := enforcement.Gate(t.Context(), makeEvent(testKind),
		makeFilter(u64(testEpoch), closedAskAndBlockSpec()), i64(10000),
		makeDeps(mock, 0, alwaysClosed), testOptions)

	if outcome.Kind != enforcement.OutcomeFailClosedDeny {
		t.Fatalf("Kind = %v, want fail-closed-deny", outcome.Kind)
	}
}

func TestGateUnknownActionIsNotABlindPermit(t *testing.T) {
	t.Parallel()

	// A future DecisionAction this Probe does not understand is unresolved, not permitted.
	mock := &mockDecider{response: makeResponse(probev1.DecisionAction(99))}
	outcome := enforcement.Gate(t.Context(), makeEvent(testKind),
		makeFilter(u64(testEpoch), askAndBlockSpec()), i64(10000), makeDeps(mock, 0, nil), testOptions)

	if outcome.Kind != enforcement.OutcomeFailOpenPermit {
		t.Fatalf("Kind = %v, want the fail mode", outcome.Kind)
	}
}

// TestGateBudgetComesFromTheInjectedClock: the budget on the wire is deadline - now, read
// only through the injected clock.
func TestGateBudgetComesFromTheInjectedClock(t *testing.T) {
	t.Parallel()

	mock := &mockDecider{response: makeResponse(probev1.DecisionAction_DECISION_ACTION_PERMIT)}
	enforcement.Gate(t.Context(), makeEvent(testKind),
		makeFilter(u64(testEpoch), askAndBlockSpec()), i64(10000), makeDeps(mock, 5000, nil), testOptions)

	request := mock.lastRequest()
	if request == nil {
		t.Fatal("no request recorded")
	}
	if request.RemainingTransportBudgetNanoseconds == nil ||
		*request.RemainingTransportBudgetNanoseconds != 5000 {
		t.Fatalf("RemainingTransportBudgetNanoseconds = %v, want 5000",
			request.RemainingTransportBudgetNanoseconds)
	}
}

func TestGateWithoutCallerBudgetUsesSpecificationBudget(t *testing.T) {
	t.Parallel()

	mock := &mockDecider{response: makeResponse(probev1.DecisionAction_DECISION_ACTION_PERMIT)}
	outcome := enforcement.Gate(t.Context(), makeEvent(testKind),
		makeFilter(u64(testEpoch), askAndBlockSpec()), nil, makeDeps(mock, 0, nil), testOptions)

	if outcome.Kind != enforcement.OutcomePermit {
		t.Fatalf("Kind = %v, want permit", outcome.Kind)
	}
	if got := mock.lastRequest().GetRemainingTransportBudgetNanoseconds(); got != 10000 {
		t.Fatalf("remaining budget = %d, want Specification budget 10000", got)
	}
}

func TestGateWithoutCallerBudgetDefersWhileSpecificationBudgetRemains(t *testing.T) {
	t.Parallel()

	mock := &mockDecider{response: makeResponse(probev1.DecisionAction_DECISION_ACTION_DEFER)}
	outcome := enforcement.Gate(t.Context(), makeEvent(testKind),
		makeFilter(u64(testEpoch), askAndBlockSpec()), nil, makeDeps(mock, 0, nil), testOptions)

	if outcome.Kind != enforcement.OutcomeDefer {
		t.Fatalf("Kind = %v, want defer", outcome.Kind)
	}
}

func TestGateWithoutBudgetTransportErrorFailsOpen(t *testing.T) {
	t.Parallel()

	mock := &mockDecider{err: errors.New("timeout")}
	outcome := enforcement.Gate(t.Context(), makeEvent(testKind),
		makeFilter(u64(testEpoch), askAndBlockSpec()), nil, makeDeps(mock, 0, nil), testOptions)

	if outcome.Kind != enforcement.OutcomeFailOpenPermit {
		t.Fatalf("Kind = %v, want fail-open-permit", outcome.Kind)
	}
}

func TestGateNoFilterHeld(t *testing.T) {
	t.Parallel()

	mock := &mockDecider{}
	outcome := enforcement.Gate(t.Context(), makeEvent(testKind), nil, i64(10000),
		makeDeps(mock, 0, nil), testOptions)

	if outcome.Kind != enforcement.OutcomeNoFilter {
		t.Fatalf("Kind = %v, want no-filter", outcome.Kind)
	}
	if outcome.FilterEpoch != nil {
		t.Fatal("no filter held means no epoch to audit")
	}
	if mock.callCount() != 0 {
		t.Fatalf("callCount = %d, want 0", mock.callCount())
	}
}

func TestGateFilterWithoutEpochIsNoFilter(t *testing.T) {
	t.Parallel()

	mock := &mockDecider{}
	outcome := enforcement.Gate(t.Context(), makeEvent(testKind),
		makeFilter(nil, askAndBlockSpec()), i64(10000), makeDeps(mock, 0, nil), testOptions)

	if outcome.Kind != enforcement.OutcomeNoFilter || mock.callCount() != 0 {
		t.Fatalf("Kind = %v callCount = %d", outcome.Kind, mock.callCount())
	}
}

// TestGateEpochZeroIsAFilter is the epoch-0 trap at the gate. `GetEpoch() == 0` here would
// silently stop enforcing for every source on epoch 0.
func TestGateEpochZeroIsAFilter(t *testing.T) {
	t.Parallel()

	mock := &mockDecider{response: makeResponse(probev1.DecisionAction_DECISION_ACTION_DENY)}
	outcome := enforcement.Gate(t.Context(), makeEvent(testKind),
		makeFilter(u64(0), askAndBlockSpec()), i64(10000), makeDeps(mock, 0, nil), testOptions)

	if outcome.Kind != enforcement.OutcomeDeny {
		t.Fatalf("Kind = %v: epoch 0 is a held filter and must be enforced", outcome.Kind)
	}
	if outcome.FilterEpoch == nil || *outcome.FilterEpoch != 0 {
		t.Fatalf("FilterEpoch = %v, want a present 0", outcome.FilterEpoch)
	}
	if mock.lastRequest().FilterEpoch == nil {
		t.Fatal("filter_epoch 0 must be present on the wire")
	}
}

func TestGateRequestCarriesIdentifiersAndEvent(t *testing.T) {
	t.Parallel()

	mock := &mockDecider{response: makeResponse(probev1.DecisionAction_DECISION_ACTION_PERMIT)}
	event := makeEvent(testKind)
	options := testOptions
	options.IdempotencyKey = "idem-key-42"

	enforcement.Gate(t.Context(), event, makeFilter(u64(testEpoch), askAndBlockSpec()),
		i64(10000), makeDeps(mock, 0, nil), options)

	request := mock.lastRequest()
	if request.GetIdempotencyKey() != "idem-key-42" {
		t.Errorf("IdempotencyKey = %q", request.GetIdempotencyKey())
	}
	if request.GetRequestId() != "req-1" {
		t.Errorf("RequestId = %q", request.GetRequestId())
	}
	if request.GetSourceHandle() != "gateway.tool-calls" {
		t.Errorf("SourceHandle = %q", request.GetSourceHandle())
	}
	if request.GetProducerEvent() != event {
		t.Error("the gate must send the event the caller projected, not a copy")
	}
	if request.FilterEpoch == nil || *request.FilterEpoch != testEpoch {
		t.Errorf("FilterEpoch = %v", request.FilterEpoch)
	}
}

func TestGateAuditsFilterEpochInEveryOutcome(t *testing.T) {
	t.Parallel()

	epoch := uint64(42)
	cases := []struct {
		name string
		mock *mockDecider
	}{
		{"permit", &mockDecider{response: makeResponse(probev1.DecisionAction_DECISION_ACTION_PERMIT)}},
		{"deny", &mockDecider{response: makeResponse(probev1.DecisionAction_DECISION_ACTION_DENY)}},
		{"defer", &mockDecider{response: makeResponse(probev1.DecisionAction_DECISION_ACTION_DEFER)}},
		{"unspecified", &mockDecider{response: makeResponse(probev1.DecisionAction_DECISION_ACTION_UNSPECIFIED)}},
		{"transport error", &mockDecider{err: errors.New("boom")}},
	}
	for _, tc := range cases {
		outcome := enforcement.Gate(t.Context(), makeEvent(testKind),
			makeFilter(&epoch, askAndBlockSpec()), i64(10000), makeDeps(tc.mock, 0, nil), testOptions)
		if outcome.FilterEpoch == nil || *outcome.FilterEpoch != epoch {
			t.Errorf("%s: FilterEpoch = %v, want %d", tc.name, outcome.FilterEpoch, epoch)
		}
		if outcome.Reason == "" {
			t.Errorf("%s: Reason must be populated on every outcome (D15)", tc.name)
		}
	}
}

// TestGateSurfacesSpecificationDecisions is divergence D11: the reference declares
// SpecificationDecision but never populates its GateOutcome with it, so an UnresolvedReason
// the endpoint returned is invisible to the host. Here it is not.
func TestGateSurfacesSpecificationDecisions(t *testing.T) {
	t.Parallel()

	reason := probev1.UnresolvedReason_UNRESOLVED_REASON_EVIDENCE_GAP
	mock := &mockDecider{response: makeResponse(
		probev1.DecisionAction_DECISION_ACTION_DEFER,
		makeDecision("spec-1", probev1.DecisionAction_DECISION_ACTION_DEFER, &reason),
	)}

	outcome := enforcement.Gate(t.Context(), makeEvent(testKind),
		makeFilter(u64(testEpoch), askAndBlockSpec()), i64(10000), makeDeps(mock, 0, nil), testOptions)

	if outcome.Kind != enforcement.OutcomeDefer {
		t.Fatalf("Kind = %v, want defer", outcome.Kind)
	}
	if len(outcome.Specifications) != 1 {
		t.Fatalf("Specifications = %v, want the per-spec decision surfaced", outcome.Specifications)
	}
	decision := outcome.Specifications[0]
	if decision.GetSpecificationId() != "spec-1" {
		t.Errorf("SpecificationId = %q", decision.GetSpecificationId())
	}
	if decision.UnresolvedReason == nil ||
		*decision.UnresolvedReason != probev1.UnresolvedReason_UNRESOLVED_REASON_EVIDENCE_GAP {
		t.Fatalf("UnresolvedReason = %v, want EVIDENCE_GAP", decision.UnresolvedReason)
	}
}

func TestGateSurfacesSpecificationDecisionsOnFailMode(t *testing.T) {
	t.Parallel()

	reason := probev1.UnresolvedReason_UNRESOLVED_REASON_TIMEOUT
	mock := &mockDecider{response: makeResponse(
		probev1.DecisionAction_DECISION_ACTION_UNSPECIFIED,
		makeDecision("spec-1", probev1.DecisionAction_DECISION_ACTION_UNSPECIFIED, &reason),
	)}

	outcome := enforcement.Gate(t.Context(), makeEvent(testKind),
		makeFilter(u64(testEpoch), askAndBlockSpec()), i64(10000), makeDeps(mock, 0, nil), testOptions)

	if outcome.Kind != enforcement.OutcomeFailOpenPermit {
		t.Fatalf("Kind = %v", outcome.Kind)
	}
	if len(outcome.Specifications) != 1 || outcome.Specifications[0].UnresolvedReason == nil {
		t.Fatal("an unresolved reason must survive into a fail-mode outcome too")
	}
}

// TestGateAlreadyCancelledContextTakesTheFailModePath is divergence D17. A dead context is a
// transport error: never a permit, never a panic, and no ask is issued.
func TestGateAlreadyCancelledContextTakesTheFailModePath(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	mock := &mockDecider{response: makeResponse(probev1.DecisionAction_DECISION_ACTION_PERMIT)}
	outcome := enforcement.Gate(ctx, makeEvent(testKind),
		makeFilter(u64(testEpoch), askAndBlockSpec()), nil, makeDeps(mock, 0, nil), testOptions)

	if outcome.Kind != enforcement.OutcomeFailOpenPermit {
		t.Fatalf("Kind = %v, want the fail mode, never a plain permit", outcome.Kind)
	}
	if mock.callCount() != 0 {
		t.Fatalf("callCount = %d, want 0 — do not ask on a dead context", mock.callCount())
	}
	if outcome.Reason == "" || outcome.Reason[:len("context-canceled")] != "context-canceled" {
		t.Fatalf("Reason = %q, want it to name the context error", outcome.Reason)
	}
}

func TestGateAlreadyCancelledContextFailsClosedWhenContracted(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	mock := &mockDecider{}
	outcome := enforcement.Gate(ctx, makeEvent(testKind),
		makeFilter(u64(testEpoch), closedAskAndBlockSpec()), nil,
		makeDeps(mock, 0, alwaysClosed), testOptions)

	if outcome.Kind != enforcement.OutcomeFailClosedDeny {
		t.Fatalf("Kind = %v, want fail-closed-deny", outcome.Kind)
	}
}

func TestGateClassifiesContextDeadlineFromDecide(t *testing.T) {
	t.Parallel()

	mock := &mockDecider{err: context.DeadlineExceeded}
	outcome := enforcement.Gate(t.Context(), makeEvent(testKind),
		makeFilter(u64(testEpoch), askAndBlockSpec()), nil, makeDeps(mock, 0, nil), testOptions)

	if outcome.Kind != enforcement.OutcomeFailOpenPermit {
		t.Fatalf("Kind = %v", outcome.Kind)
	}
	if outcome.Reason[:len("context-deadline-exceeded")] != "context-deadline-exceeded" {
		t.Fatalf("Reason = %q, want the context deadline named distinctly from a Connect code",
			outcome.Reason)
	}
}

func TestGateClassifiesConnectErrors(t *testing.T) {
	t.Parallel()

	mock := &mockDecider{err: connect.NewError(connect.CodeUnavailable, errors.New("endpoint down"))}
	outcome := enforcement.Gate(t.Context(), makeEvent(testKind),
		makeFilter(u64(testEpoch), askAndBlockSpec()), i64(10000), makeDeps(mock, 0, nil), testOptions)

	if outcome.Kind != enforcement.OutcomeFailOpenPermit {
		t.Fatalf("Kind = %v", outcome.Kind)
	}
	if outcome.Reason[:len("connect-unavailable")] != "connect-unavailable" {
		t.Fatalf("Reason = %q, want the Connect code for auditability", outcome.Reason)
	}
}

func TestOutcomeKindString(t *testing.T) {
	t.Parallel()

	cases := map[enforcement.OutcomeKind]string{
		enforcement.OutcomePermit:         "permit",
		enforcement.OutcomeDeny:           "deny",
		enforcement.OutcomeDefer:          "defer",
		enforcement.OutcomeFailOpenPermit: "fail-open-permit",
		enforcement.OutcomeFailClosedDeny: "fail-closed-deny",
		enforcement.OutcomeNoFilter:       "no-filter",
		enforcement.OutcomeUnspecified:    "unspecified",
	}
	for kind, want := range cases {
		if kind.String() != want {
			t.Errorf("String() = %q, want %q", kind.String(), want)
		}
	}
	// The zero value must never read as permitted: a GateOutcome a caller forgot to fill in
	// should not accidentally authorise anything.
	if enforcement.OutcomeUnspecified.Permitted() {
		t.Fatal("the zero OutcomeKind must not be permitted")
	}
}

func TestRemainingTransportBudgetNs(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name          string
		deadline, now int64
		want          uint64
	}{
		{"budget remains", 10000, 5000, 5000},
		{"exactly exhausted", 10000, 10000, 0},
		{"deadline passed", 10000, 99999, 0},
		{"full budget", 10000, 0, 10000},
		{"negative clock origin", 0, -5000, 5000},
		{"signed boundary wrap", -9223372036854775804, 9223372036854775802, 10},
		{"ambiguous half range", -9223372036854775808, 0, 0},
	}
	for _, tc := range cases {
		if got := enforcement.RemainingTransportBudgetNs(tc.deadline, tc.now); got != tc.want {
			t.Errorf("%s: RemainingTransportBudgetNs(%d, %d) = %d, want %d",
				tc.name, tc.deadline, tc.now, got, tc.want)
		}
	}
}

func TestGateExcludesInactiveEnforcementEntries(t *testing.T) {
	cases := []struct {
		mode      modelv1.EvaluationMode
		readiness modelv1.Readiness
	}{
		{modelv1.EvaluationMode_EVALUATION_MODE_SHADOW, modelv1.Readiness_READINESS_ACTIVE},
		{modelv1.EvaluationMode_EVALUATION_MODE_DETECT, modelv1.Readiness_READINESS_ACTIVE},
		{modelv1.EvaluationMode_EVALUATION_MODE_ENFORCE, modelv1.Readiness_READINESS_WARMING},
	}
	for _, tc := range cases {
		spec := closedAskAndBlockSpec()
		spec.EvaluationMode, spec.Readiness = tc.mode, tc.readiness
		mock := &mockDecider{}
		outcome := enforcement.Gate(t.Context(), makeEvent(testKind), makeFilter(u64(testEpoch), spec), nil, makeDeps(mock, 0, alwaysClosed), testOptions)
		if outcome.Kind != enforcement.OutcomePermit || mock.callCount() != 0 {
			t.Fatalf("inactive entry produced %v and %d calls", outcome.Kind, mock.callCount())
		}
	}
}

func TestGateMissingEligibleBudgetExhaustsWithoutClockOrDecide(t *testing.T) {
	spec := askAndBlockSpec()
	spec.LatencyBudgetNanoseconds = nil
	clockCalls := 0
	mock := &mockDecider{}
	deps := makeDeps(mock, 0, nil)
	deps.NowMonotonicNs = func() int64 { clockCalls++; return 0 }
	outcome := enforcement.Gate(t.Context(), makeEvent(testKind), makeFilter(u64(testEpoch), spec), nil, deps, testOptions)
	if outcome.Kind != enforcement.OutcomeFailOpenPermit || clockCalls != 0 || mock.callCount() != 0 {
		t.Fatalf("outcome=%v clock=%d decide=%d", outcome.Kind, clockCalls, mock.callCount())
	}
}

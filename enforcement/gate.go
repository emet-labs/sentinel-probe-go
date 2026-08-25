package enforcement

import (
	"context"
	"errors"
	"fmt"

	"connectrpc.com/connect"

	modelv1 "github.com/emet-labs/sentinel/sdk/go/gen/sentinel/model/v1"
	probev1 "github.com/emet-labs/sentinel/sdk/go/gen/sentinel/probe/v1"
	"github.com/emet-labs/sentinel/sdk/go/internal/specmatch"
)

// OutcomeKind enumerates the actions a Probe can be told to take. Analog of the discriminant
// of the TypeScript GateOutcome union.
type OutcomeKind int

const (
	// OutcomeUnspecified is the zero value and is never returned. It exists so that a
	// GateOutcome accidentally constructed by a caller does not read as a permit.
	OutcomeUnspecified OutcomeKind = iota
	// OutcomePermit means proceed. Either the decision endpoint permitted, or no enforcing
	// Specification selected the event.
	OutcomePermit
	// OutcomeDeny means block: the decision endpoint denied.
	OutcomeDeny
	// OutcomeDefer means the decision is unresolved and budget remains, so the host may wait
	// or retry.
	OutcomeDefer
	// OutcomeFailOpenPermit means the ask could not be resolved and the aggregate fail mode
	// is OPEN, so the Probe proceeds.
	OutcomeFailOpenPermit
	// OutcomeFailClosedDeny means the ask could not be resolved and some enforcing spec is
	// contracted fail-closed, so the Probe blocks.
	OutcomeFailClosedDeny
	// OutcomeNoFilter means no filter or no filter epoch is held, so there is nothing to
	// enforce and no ask is made. A discriminant of its own rather than a fail-open, so an
	// auditor can tell "we had no policy" from "we had policy and could not reach Sentinel".
	OutcomeNoFilter
)

// String makes failure messages and audit records readable.
func (k OutcomeKind) String() string {
	switch k {
	case OutcomePermit:
		return "permit"
	case OutcomeDeny:
		return "deny"
	case OutcomeDefer:
		return "defer"
	case OutcomeFailOpenPermit:
		return "fail-open-permit"
	case OutcomeFailClosedDeny:
		return "fail-closed-deny"
	case OutcomeNoFilter:
		return "no-filter"
	case OutcomeUnspecified:
		return "unspecified"
	default:
		return fmt.Sprintf("OutcomeKind(%d)", int(k))
	}
}

// Permitted reports whether the Probe may proceed. Both permit and fail-open-permit allow the
// action; deny, fail-closed-deny and defer do not. no-filter permits, matching the reference's
// conservative default of not blocking on absent policy.
func (k OutcomeKind) Permitted() bool {
	switch k {
	case OutcomePermit, OutcomeFailOpenPermit, OutcomeNoFilter:
		return true
	default:
		return false
	}
}

// GateOutcome is what the Probe must do, plus the evidence for why.
//
// Divergence D15: Reason is populated on every outcome. The TypeScript reference carries a
// reason only on the two fail-mode arms of a discriminated union; Go has no sum types, so a
// single struct is forced, and leaving Reason empty on the non-fail arms would be strictly
// less auditable for no gain.
//
// Divergence D11: Specifications carries the per-Specification decisions the endpoint
// returned, including UnresolvedReason. The reference declares SpecificationDecision but
// never populates it, so this is an addition, not a port.
type GateOutcome struct {
	Kind   OutcomeKind
	Reason string
	// FilterEpoch is the epoch the decision was taken against, audited in every outcome. nil
	// only for OutcomeNoFilter, where by definition none was held.
	FilterEpoch *uint64
	// Specifications are the per-spec decisions from the response, empty when no ask was made.
	Specifications []*probev1.SpecificationDecision
}

// Deps are the effects the gate needs, all injected so the gate itself stays pure and
// testable without a network or a clock.
type Deps struct {
	// Decide performs the ask. In production this wraps
	// probev1connect.SentinelDecisionServiceClient.Decide; in tests it is a stub.
	Decide func(ctx context.Context, request *probev1.DecideRequest) (*probev1.DecideResponse, error)
	// NowMonotonicNs reads the host's monotonic clock. Required whenever deadlineNs is set.
	NowMonotonicNs func() int64
	// AcceptedFailModeFor reports the fail mode the deployment has actually contracted for a
	// spec. A spec declaring CLOSED without an accepted contract downgrades to OPEN: an
	// operator must have agreed to be blocked.
	//
	// REQUIRED whenever an enforcing Specification selects the event. Gate panics when it is
	// nil, exactly as it already would on a nil Decide or a nil NowMonotonicNs with a
	// deadline set. Treating nil as "nothing accepted" would make this the one dependency
	// whose absence silently WEAKENS enforcement: a deployment that wires up Decide and
	// forgets this one would get a gate that can never fail closed, with no signal at all.
	// The TypeScript reference makes the field required and computes the aggregate fail mode
	// outside its try/catch (enforcement-gate.ts:82, before the try at :108), so a missing
	// implementation throws to the caller there too rather than permitting.
	AcceptedFailModeFor func(spec *modelv1.SpecificationFilter) modelv1.FailMode
}

// Options carry the per-call identifiers stamped into the DecideRequest.
type Options struct {
	SourceHandle   string
	RequestID      string
	IdempotencyKey string
}

// Gate enforces one ASK_AND_BLOCK event and returns the action the Probe must take.
//
// Control flow mirrors enforcement-gate.ts:57-201 step for step:
//
//  1. no filter, or a filter with no epoch, returns OutcomeNoFilter without asking;
//  2. the enforcing set is the specs that select the event AND declare ASK_AND_BLOCK
//     delivery; an empty set means the event is ship-async and the gate is a no-op permit;
//  3. the aggregate fail mode is CLOSED iff some enforcing spec declares CLOSED and the
//     deployment has accepted CLOSED for it — fail-closed wins over any number of open specs;
//  4. if a deadline was set and the budget is already exhausted, apply the fail mode WITHOUT
//     asking;
//  5. build the DecideRequest from the event the caller passed, which the caller has already
//     projected through filter.ApplyFilter — Gate never projects;
//  6. ask; any error, including a context error, applies the aggregate fail mode;
//  7. PERMIT permits, DENY denies, DEFER defers while budget remains or no deadline was set
//     and otherwise applies the fail mode, and UNSPECIFIED applies the fail mode — never a
//     blind permit.
//
// filter is the caller's snapshot from client.FilterStore.Get(). Gate holds no mutable state
// and is safe to call from many goroutines.
func Gate(
	ctx context.Context,
	event *modelv1.ProducerEvent,
	filter *modelv1.EventFilter,
	deadlineNs *int64,
	deps Deps,
	options Options,
) GateOutcome {
	// 1. No-filter guard. Presence is `Epoch == nil`: epoch 0 is a legitimate epoch, so
	//    GetEpoch() == 0 would misclassify it as "no policy held".
	if filter == nil || filter.Epoch == nil {
		return GateOutcome{Kind: OutcomeNoFilter, Reason: "no-filter"}
	}
	filterEpoch := filter.Epoch

	// 2. Enforcing set: selects the event AND asks-and-blocks.
	var enforcing []*modelv1.SpecificationFilter
	for _, spec := range filter.GetSpecifications() {
		if specmatch.Selects(spec, event) && isEnforceable(spec) {
			enforcing = append(enforcing, spec)
		}
	}
	if len(enforcing) == 0 {
		return GateOutcome{Kind: OutcomePermit, Reason: "no-ask-and-block-spec", FilterEpoch: filterEpoch}
	}

	// 3. Aggregate fail mode: fail-closed wins.
	aggregateFailMode := computeAggregateFailMode(enforcing, deps)
	minimum := ^uint64(0)
	for _, spec := range enforcing {
		if spec.LatencyBudgetNanoseconds == nil || spec.GetLatencyBudgetNanoseconds() == 0 {
			return applyFailMode(aggregateFailMode, "budget-exhausted", filterEpoch, nil)
		}
		if spec.GetLatencyBudgetNanoseconds() < minimum {
			minimum = spec.GetLatencyBudgetNanoseconds()
		}
	}
	if deps.NowMonotonicNs == nil {
		return applyFailMode(aggregateFailMode, "clock-unavailable", filterEpoch, nil)
	}
	anchor := deps.NowMonotonicNs()
	if deadlineNs != nil {
		caller, valid := monotonicDelta(anchor, *deadlineNs)
		if !valid {
			caller = 0
		}
		if caller < minimum {
			minimum = caller
		}
	}
	state := budgetState{anchor: anchor, budget: minimum}

	// 4. Budget. Exhausted before the call means apply the fail mode without asking, so a
	//    Probe that is already out of time does not spend more of it.
	remaining := state.remaining(deps.NowMonotonicNs())
	if remaining == 0 {
		return applyFailMode(aggregateFailMode, "budget-exhausted", filterEpoch, nil)
	}
	remainingBudget := &remaining

	// 5. Build the request from the already-projected event.
	request := &probev1.DecideRequest{
		RequestId:                           options.RequestID,
		IdempotencyKey:                      options.IdempotencyKey,
		SourceHandle:                        options.SourceHandle,
		FilterEpoch:                         filterEpoch,
		ProducerEvent:                       event,
		RemainingTransportBudgetNanoseconds: remainingBudget,
	}

	// 6. Ask. A context that is already dead is classified as a transport error and routed
	//    into the fail mode WITHOUT asking (divergence D17): a real Connect client over
	//    net/http would fail the same way, and checking here makes the outcome independent of
	//    whether a given Decide implementation happens to honour ctx. Never a permit, never a
	//    panic. The TypeScript reference has no context to check.
	if err := ctx.Err(); err != nil {
		return applyFailMode(aggregateFailMode, describeError(err), filterEpoch, nil)
	}
	response, err := deps.Decide(ctx, request)
	if err != nil {
		return applyFailMode(aggregateFailMode, describeError(err), filterEpoch, nil)
	}

	// 7. Honour the answer.
	return handleResponse(response, aggregateFailMode, filterEpoch, state, deps)
}

func handleResponse(
	response *probev1.DecideResponse,
	aggregateFailMode modelv1.FailMode,
	filterEpoch *uint64,
	state budgetState,
	deps Deps,
) GateOutcome {
	decisions := response.GetSpecifications()

	switch response.GetAction() {
	case probev1.DecisionAction_DECISION_ACTION_PERMIT:
		return GateOutcome{Kind: OutcomePermit, Reason: "permit", FilterEpoch: filterEpoch, Specifications: decisions}

	case probev1.DecisionAction_DECISION_ACTION_DENY:
		return GateOutcome{Kind: OutcomeDeny, Reason: "deny", FilterEpoch: filterEpoch, Specifications: decisions}

	case probev1.DecisionAction_DECISION_ACTION_DEFER:
		if state.remaining(deps.NowMonotonicNs()) > 0 {
			return GateOutcome{Kind: OutcomeDefer, Reason: "defer", FilterEpoch: filterEpoch, Specifications: decisions}
		}
		return applyFailMode(aggregateFailMode, "defer-budget-exhausted", filterEpoch, decisions)

	case probev1.DecisionAction_DECISION_ACTION_UNSPECIFIED:
		return applyFailMode(aggregateFailMode, "unspecified-action", filterEpoch, decisions)

	default:
		// A future action this Probe does not understand is unresolved, not permitted.
		return applyFailMode(aggregateFailMode, "unspecified-action", filterEpoch, decisions)
	}
}

func applyFailMode(
	failMode modelv1.FailMode,
	reason string,
	filterEpoch *uint64,
	decisions []*probev1.SpecificationDecision,
) GateOutcome {
	kind := OutcomeFailOpenPermit
	if failMode == modelv1.FailMode_FAIL_MODE_CLOSED {
		kind = OutcomeFailClosedDeny
	}
	return GateOutcome{Kind: kind, Reason: reason, FilterEpoch: filterEpoch, Specifications: decisions}
}

// computeAggregateFailMode returns CLOSED iff some enforcing spec both DECLARES CLOSED and has
// CLOSED accepted by the deployment. Declaration alone downgrades to OPEN: blocking a caller
// is something an operator has to have agreed to. FAIL_MODE_UNSPECIFIED is OPEN.
//
// Panics on a nil Deps.AcceptedFailModeFor. See the field's documentation: a missing contract
// source is a wiring bug, and defaulting it to OPEN would silently disable fail-closed for
// every Specification — the exact failure this package exists to prevent. It is only reached
// when the enforcing set is non-empty, so a Probe with no ask-and-block Specifications never
// needs the dependency.
func computeAggregateFailMode(enforcing []*modelv1.SpecificationFilter, deps Deps) modelv1.FailMode {
	if deps.AcceptedFailModeFor == nil {
		panic("enforcement: Deps.AcceptedFailModeFor is required when an enforcing " +
			"Specification selects the event; a nil contract source cannot be defaulted to " +
			"fail-open without silently disabling fail-closed enforcement")
	}
	for _, spec := range enforcing {
		if spec.GetFailMode() != modelv1.FailMode_FAIL_MODE_CLOSED {
			continue
		}
		if deps.AcceptedFailModeFor(spec) == modelv1.FailMode_FAIL_MODE_CLOSED {
			return modelv1.FailMode_FAIL_MODE_CLOSED
		}
	}
	return modelv1.FailMode_FAIL_MODE_OPEN
}

func isEnforceable(spec *modelv1.SpecificationFilter) bool {
	return spec.GetEventMatch().GetDeliveryMode() == modelv1.DeliveryMode_DELIVERY_MODE_ASK_AND_BLOCK &&
		spec.GetEvaluationMode() == modelv1.EvaluationMode_EVALUATION_MODE_ENFORCE &&
		spec.GetReadiness() == modelv1.Readiness_READINESS_ACTIVE
}

// describeError renders a transport failure for the audit record, distinguishing a context
// error from a Connect status so an operator can tell "the host gave up" from "the endpoint
// said no". Every error class still routes into the fail mode.
func describeError(err error) string {
	switch {
	case errors.Is(err, context.DeadlineExceeded):
		return "context-deadline-exceeded: " + err.Error()
	case errors.Is(err, context.Canceled):
		return "context-canceled: " + err.Error()
	}
	var connectErr *connect.Error
	if errors.As(err, &connectErr) {
		return "connect-" + connect.CodeOf(err).String() + ": " + err.Error()
	}
	return "transport-error: " + err.Error()
}

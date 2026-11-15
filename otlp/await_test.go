package otlp_test

import (
	"context"
	"errors"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/emet-labs/sentinel-probe-go/client"
	"github.com/emet-labs/sentinel-probe-go/enforcement"
	modelv1 "github.com/emet-labs/sentinel-probe-go/gen/sentinel/model/v1"
	probev1 "github.com/emet-labs/sentinel-probe-go/gen/sentinel/probe/v1"
	"github.com/emet-labs/sentinel-probe-go/otlp"
)

// scriptedTimer is an injected decision timer. Each arm records the budget and
// runs onArm, which may deliver a log or expire the wait, so no test reads a
// wall clock or sleeps.
type scriptedTimer struct {
	armed []time.Duration
	onArm func(expire func())
}

func (s *scriptedTimer) arm(budget time.Duration) (<-chan time.Time, func() bool) {
	s.armed = append(s.armed, budget)
	fired := make(chan time.Time, 1)
	if s.onArm != nil {
		s.onArm(func() { fired <- time.Time{} })
	}
	return fired, func() bool { return true }
}

func listenScripted(t *testing.T, timer *scriptedTimer) *otlp.Receiver {
	t.Helper()
	receiver, err := otlp.ListenWith(otlp.ListenConfig{DecisionTimer: timer.arm})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = receiver.Close() })
	return receiver
}

func TestAwaitReturnsAnAnswerThatArrivesAfterTheAsk(t *testing.T) {
	timer := &scriptedTimer{}
	receiver := listenScripted(t, timer)
	timer.onArm = func(func()) {
		postLogs(t, receiver.LogsURL(), decideExport(t, "req-late", probev1.DecisionAction_DECISION_ACTION_DENY), http.StatusOK)
	}
	response, err := receiver.Await(t.Context(), "req-late", 40*time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	if response.GetAction() != probev1.DecisionAction_DECISION_ACTION_DENY {
		t.Fatalf("action = %v", response.GetAction())
	}
	if len(timer.armed) != 1 || timer.armed[0] != 40*time.Millisecond {
		t.Fatalf("armed = %v, want one 40ms wait", timer.armed)
	}
}

func TestAwaitReturnsAStoredAnswerWithoutArming(t *testing.T) {
	timer := &scriptedTimer{}
	receiver := listenScripted(t, timer)
	postLogs(t, receiver.LogsURL(), decideExport(t, "req-early", probev1.DecisionAction_DECISION_ACTION_PERMIT), http.StatusOK)
	for _, budget := range []time.Duration{0, 40 * time.Millisecond} {
		response, err := receiver.Await(t.Context(), "req-early", budget)
		if err != nil {
			t.Fatal(err)
		}
		if response.GetAction() != probev1.DecisionAction_DECISION_ACTION_PERMIT {
			t.Fatalf("action = %v", response.GetAction())
		}
	}
	if len(timer.armed) != 0 {
		t.Fatalf("armed = %v, want none", timer.armed)
	}
}

func TestAwaitWithoutBudgetDoesNotWait(t *testing.T) {
	timer := &scriptedTimer{}
	receiver := listenScripted(t, timer)
	_, err := receiver.Await(t.Context(), "req-none", 0)
	assertRefusal(t, err, "missing-decision")
	if len(timer.armed) != 0 {
		t.Fatalf("armed = %v, want none", timer.armed)
	}
}

func TestAwaitExpiresAndALateAnswerDoesNotChangeTheAsk(t *testing.T) {
	timer := &scriptedTimer{onArm: func(expire func()) { expire() }}
	receiver := listenScripted(t, timer)
	_, err := receiver.Await(t.Context(), "req-expired", 40*time.Millisecond)
	assertRefusal(t, err, "timeout")
	postLogs(t, receiver.LogsURL(), decideExport(t, "req-expired", probev1.DecisionAction_DECISION_ACTION_PERMIT), http.StatusOK)
}

func TestAwaitIgnoresAnAnswerForAnotherRequest(t *testing.T) {
	timer := &scriptedTimer{}
	receiver := listenScripted(t, timer)
	timer.onArm = func(expire func()) {
		postLogs(t, receiver.LogsURL(), decideExport(t, "req-other", probev1.DecisionAction_DECISION_ACTION_DENY), http.StatusOK)
		expire()
	}
	_, err := receiver.Await(t.Context(), "req-mine", 40*time.Millisecond)
	assertRefusal(t, err, "timeout")
}

func TestAwaitFailsAtOnceOnARefusedDecisionForTheRequest(t *testing.T) {
	timer := &scriptedTimer{}
	receiver := listenScripted(t, timer)
	timer.onArm = func(func()) {
		postLogs(t, receiver.LogsURL(), decideExport(t, "req-bad", probev1.DecisionAction_DECISION_ACTION_UNSPECIFIED), http.StatusBadRequest)
	}
	_, err := receiver.Await(t.Context(), "req-bad", 40*time.Millisecond)
	assertRefusal(t, err, "unspecified-action")
	var refused *otlp.DecisionRefusal
	if !errors.As(err, &refused) || refused.RequestID != "req-bad" {
		t.Fatalf("err = %v, want a DecisionRefusal for req-bad", err)
	}

	// A later valid answer clears the refusal.
	postLogs(t, receiver.LogsURL(), decideExport(t, "req-bad", probev1.DecisionAction_DECISION_ACTION_DENY), http.StatusOK)
	response, err := receiver.Await(t.Context(), "req-bad", 0)
	if err != nil || response.GetAction() != probev1.DecisionAction_DECISION_ACTION_DENY {
		t.Fatalf("response = %v, err = %v", response, err)
	}
}

func TestCloseFailsAPendingAwait(t *testing.T) {
	timer := &scriptedTimer{}
	receiver := listenScripted(t, timer)
	timer.onArm = func(func()) { _ = receiver.Close() }
	_, err := receiver.Await(t.Context(), "req-closed", 40*time.Millisecond)
	assertRefusal(t, err, "receiver-closed")
	_, err = receiver.Await(t.Context(), "req-after", 40*time.Millisecond)
	assertRefusal(t, err, "receiver-closed")
}

func TestAwaitEndsWithTheContext(t *testing.T) {
	timer := &scriptedTimer{}
	receiver := listenScripted(t, timer)
	ctx, cancel := context.WithCancel(t.Context())
	timer.onArm = func(func()) { cancel() }
	_, err := receiver.Await(ctx, "req-ctx", 40*time.Millisecond)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v, want context.Canceled", err)
	}
}

// TestGateWaitsForTheDecisionWithinTheRemainingBudget runs the production
// adapter, ProbeClient.DecideFunc over the receiver, through the gate. The open
// spec has a 50ms budget; the clock reads 1ms then 11ms, so 40ms remain.
func TestGateWaitsForTheDecisionWithinTheRemainingBudget(t *testing.T) {
	cases := []struct {
		name     string
		failMode modelv1.FailMode
		deliver  bool
		want     enforcement.OutcomeKind
	}{
		{"answer-after-ask", modelv1.FailMode_FAIL_MODE_OPEN, true, enforcement.OutcomeDeny},
		{"receiver-timeout-fail-open", modelv1.FailMode_FAIL_MODE_OPEN, false, enforcement.OutcomeFailOpenPermit},
		{"receiver-timeout-fail-closed", modelv1.FailMode_FAIL_MODE_CLOSED, false, enforcement.OutcomeFailClosedDeny},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			timer := &scriptedTimer{}
			receiver := listenScripted(t, timer)
			timer.onArm = func(expire func()) {
				if tc.deliver {
					postLogs(t, receiver.LogsURL(), readGolden(t, "decide-response.bin"), http.StatusOK)
					return
				}
				expire()
			}
			probe := client.New(client.Config{SourceHandle: "sagashop-order"})
			probe.SetDecisions(receiver)
			reads := []int64{1_000_000, 11_000_000}
			outcome := enforcement.Gate(t.Context(), gateEvent(), waitFilter(tc.failMode), nil, enforcement.Deps{
				Decide: probe.DecideFunc(),
				NowMonotonicNs: func() int64 {
					next := reads[0]
					reads = reads[1:]
					return next
				},
				AcceptedFailModeFor: func(spec *modelv1.SpecificationFilter) modelv1.FailMode {
					return spec.GetFailMode()
				},
			}, enforcement.Options{SourceHandle: "sagashop-order", RequestID: "req-charge-1", IdempotencyKey: "idem-1"})
			if outcome.Kind != tc.want {
				t.Fatalf("kind = %v (%s), want %v", outcome.Kind, outcome.Reason, tc.want)
			}
			if len(reads) != 0 {
				t.Fatalf("%d clock reads left unconsumed", len(reads))
			}
			if len(timer.armed) != 1 || timer.armed[0] != 40*time.Millisecond {
				t.Fatalf("armed = %v, want one 40ms wait", timer.armed)
			}
		})
	}
}

func TestDecideFuncCapsAnOversizedBudget(t *testing.T) {
	timer := &scriptedTimer{onArm: func(expire func()) { expire() }}
	receiver := listenScripted(t, timer)
	probe := client.New(client.Config{SourceHandle: "sagashop-order"})
	probe.SetDecisions(receiver)
	budget := ^uint64(0)
	_, err := probe.DecideFunc()(t.Context(), &probev1.DecideRequest{
		RequestId:                           "req-huge",
		RemainingTransportBudgetNanoseconds: &budget,
	})
	assertRefusal(t, err, "timeout")
	if len(timer.armed) != 1 || timer.armed[0] != time.Duration(1<<63-1) {
		t.Fatalf("armed = %v, want the maximum Duration", timer.armed)
	}
	_, err = probe.DecideFunc()(t.Context(), &probev1.DecideRequest{RequestId: "req-absent"})
	assertRefusal(t, err, "missing-decision")
	if len(timer.armed) != 1 {
		t.Fatalf("an absent budget armed a wait: %v", timer.armed)
	}
}

func waitFilter(failMode modelv1.FailMode) *modelv1.EventFilter {
	filter := gateFilter()
	budget := uint64(50_000_000)
	spec := filter.GetSpecifications()[0]
	spec.LatencyBudgetNanoseconds = &budget
	spec.FailMode = failMode
	return filter
}

func assertRefusal(t *testing.T, err error, reason string) {
	t.Helper()
	var refusal *otlp.Refusal
	if !errors.As(err, &refusal) || refusal.Reason != reason {
		t.Fatalf("err = %v, want refusal %q", err, reason)
	}
	if !strings.Contains(err.Error(), reason) {
		t.Fatalf("err = %q does not name %q", err, reason)
	}
}

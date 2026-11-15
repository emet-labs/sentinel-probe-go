package conformance

import (
	"bytes"
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/emet-labs/sentinel-probe-go/client"
	"github.com/emet-labs/sentinel-probe-go/enforcement"
	modelv1 "github.com/emet-labs/sentinel-probe-go/gen/sentinel/model/v1"
	probev1 "github.com/emet-labs/sentinel-probe-go/gen/sentinel/probe/v1"
	"github.com/emet-labs/sentinel-probe-go/otlp"
)

// The decision_wait suite (manifest-v1.json, 1.4.0) runs through the production log Receiver
// and ProbeClient.DecideFunc. Only ListenConfig.DecisionTimer is injected: `deliver` serves the
// answer golden in-process from inside the timer, `expire` returns an already-fired channel.
// Await re-reads the stored answer after its select, so ["deliver","expire"] is deterministic.

type decisionWaitCase struct {
	ID        string          `json:"id"`
	Via       string          `json:"via"`
	RequestID string          `json:"request_id"`
	Filter    *gateCaseFilter `json:"filter"`
	Event     *struct {
		Kind string `json:"kind"`
	} `json:"event"`
	LocalDeadlineNS            *string  `json:"local_deadline_ns"`
	ClockReadsNS               []string `json:"clock_reads_ns"`
	RemainingTransportBudgetNS *string  `json:"remaining_transport_budget_ns"`
	Answer                     string   `json:"answer"`
	BeforeAsk                  []string `json:"before_ask"`
	OnWait                     []string `json:"on_wait"`
	Expected                   struct {
		Kind                       string  `json:"kind"`
		DecideCalls                int     `json:"decide_calls"`
		RemainingTransportBudgetNS string  `json:"remaining_transport_budget_ns"`
		Result                     string  `json:"result"`
		ArmedWaitNS                *string `json:"armed_wait_ns"`
	} `json:"expected"`
}

type gateCaseFilter struct {
	Epoch          string `json:"epoch"`
	Specifications []struct {
		ID               string   `json:"id"`
		EventKinds       []string `json:"event_kinds"`
		DeliveryMode     string   `json:"delivery_mode"`
		EvaluationMode   string   `json:"evaluation_mode"`
		Readiness        string   `json:"readiness"`
		LatencyBudgetNS  string   `json:"latency_budget_ns"`
		FailMode         string   `json:"fail_mode"`
		AcceptedFailMode string   `json:"accepted_fail_mode"`
	} `json:"specifications"`
}

func manifestSuitePath(t *testing.T, kind string) string {
	t.Helper()
	var manifest struct {
		FormatVersion string `json:"format_version"`
		Suites        []struct {
			Kind string `json:"kind"`
			Path string `json:"path"`
		} `json:"suites"`
		Malformed []struct {
			Path              string `json:"path"`
			RejectionCategory string `json:"rejection_category"`
		} `json:"malformed"`
	}
	decodeStrict(t, "manifest-v1.json", &manifest)
	if manifest.FormatVersion != "1.4.0" {
		t.Fatalf("manifest format_version = %s", manifest.FormatVersion)
	}
	path := ""
	for _, entry := range manifest.Suites {
		if entry.Kind == kind {
			if path != "" {
				t.Fatalf("more than one %s suite", kind)
			}
			path = entry.Path
		}
	}
	if path == "" {
		t.Fatalf("no %s suite", kind)
	}
	return path
}

func TestDecisionWaitVectors(t *testing.T) {
	var suite struct {
		FormatVersion string             `json:"format_version"`
		Kind          string             `json:"kind"`
		Cases         []decisionWaitCase `json:"cases"`
	}
	decodeStrict(t, manifestSuitePath(t, "decision_wait"), &suite)
	if suite.FormatVersion != "1.2.0" || suite.Kind != "decision_wait" || len(suite.Cases) == 0 {
		t.Fatal("unsupported decision_wait suite")
	}
	for _, vector := range suite.Cases {
		vector := vector
		t.Run(vector.ID, func(t *testing.T) {
			answer, err := os.ReadFile(filepath.Join(fixtureRoot(t), vector.Answer))
			if err != nil {
				t.Fatal(err)
			}
			var armed []time.Duration
			var receiver *otlp.Receiver
			timer := func(budget time.Duration) (<-chan time.Time, func() bool) {
				armed = append(armed, budget)
				never := make(chan time.Time)
				var fired <-chan time.Time = never
				for _, step := range vector.OnWait {
					switch step {
					case "deliver":
						deliverAnswer(t, receiver, answer)
					case "expire":
						done := make(chan time.Time, 1)
						done <- time.Time{}
						fired = done
					default:
						t.Fatalf("unknown on_wait step %s", step)
					}
				}
				return fired, func() bool { return true }
			}
			receiver, err = otlp.ListenWith(otlp.ListenConfig{DecisionTimer: timer})
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = receiver.Close() })
			for _, step := range vector.BeforeAsk {
				if step != "deliver" {
					t.Fatalf("unknown before_ask step %s", step)
				}
				deliverAnswer(t, receiver, answer)
			}
			probe := client.New(client.Config{SourceHandle: "fixture-source"})
			probe.SetDecisions(receiver)
			decide := probe.DecideFunc()

			switch vector.Via {
			case "gate":
				runDecisionWaitGate(t, vector, decide)
			case "adapter":
				request := &probev1.DecideRequest{RequestId: vector.RequestID}
				if vector.RemainingTransportBudgetNS != nil {
					budget := parseUint64(t, *vector.RemainingTransportBudgetNS)
					request.RemainingTransportBudgetNanoseconds = &budget
				}
				result := "error"
				if response, err := decide(context.Background(), request); err == nil {
					result = actionToken(response.GetAction())
				}
				if result != vector.Expected.Result {
					t.Errorf("result = %s, want %s", result, vector.Expected.Result)
				}
			default:
				t.Fatalf("unknown via %s", vector.Via)
			}
			var want []time.Duration
			if vector.Expected.ArmedWaitNS != nil {
				want = []time.Duration{time.Duration(parseUint64(t, *vector.Expected.ArmedWaitNS))}
			}
			if len(armed) != len(want) || (len(want) == 1 && armed[0] != want[0]) {
				t.Errorf("armed waits = %v, want %v", armed, want)
			}
		})
	}
}

func runDecisionWaitGate(
	t *testing.T,
	vector decisionWaitCase,
	decide func(context.Context, *probev1.DecideRequest) (*probev1.DecideResponse, error),
) {
	t.Helper()
	epoch := parseUint64(t, vector.Filter.Epoch)
	filter := &modelv1.EventFilter{Epoch: &epoch}
	accepted := map[string]modelv1.FailMode{}
	for _, fixture := range vector.Filter.Specifications {
		failMode := modelv1.FailMode_FAIL_MODE_OPEN
		if fixture.FailMode == "closed" {
			failMode = modelv1.FailMode_FAIL_MODE_CLOSED
		}
		acceptedMode := modelv1.FailMode_FAIL_MODE_OPEN
		if fixture.AcceptedFailMode == "closed" {
			acceptedMode = modelv1.FailMode_FAIL_MODE_CLOSED
		}
		accepted[fixture.ID] = acceptedMode
		delivery := modelv1.DeliveryMode_DELIVERY_MODE_SHIP_ASYNC
		if fixture.DeliveryMode == "ask_and_block" {
			delivery = modelv1.DeliveryMode_DELIVERY_MODE_ASK_AND_BLOCK
		}
		budget := parseUint64(t, fixture.LatencyBudgetNS)
		filter.Specifications = append(filter.Specifications, &modelv1.SpecificationFilter{SpecificationId: fixture.ID, FailMode: failMode, EvaluationMode: modelv1.EvaluationMode_EVALUATION_MODE_ENFORCE, Readiness: modelv1.Readiness_READINESS_ACTIVE, LatencyBudgetNanoseconds: &budget, EventMatch: &modelv1.EventMatch{EventKinds: fixture.EventKinds, DeliveryMode: delivery}})
	}
	reads := make([]int64, len(vector.ClockReadsNS))
	for index, raw := range vector.ClockReadsNS {
		value, err := strconv.ParseInt(raw, 10, 64)
		if err != nil {
			t.Fatal(err)
		}
		reads[index] = value
	}
	readIndex := 0
	var requests []*probev1.DecideRequest
	deps := enforcement.Deps{
		NowMonotonicNs: func() int64 {
			if readIndex >= len(reads) {
				t.Fatal("clock script exhausted")
			}
			value := reads[readIndex]
			readIndex++
			return value
		},
		AcceptedFailModeFor: func(spec *modelv1.SpecificationFilter) modelv1.FailMode { return accepted[spec.GetSpecificationId()] },
		Decide: func(ctx context.Context, request *probev1.DecideRequest) (*probev1.DecideResponse, error) {
			requests = append(requests, request)
			return decide(ctx, request)
		},
	}
	var deadline *int64
	if vector.LocalDeadlineNS != nil {
		value, err := strconv.ParseInt(*vector.LocalDeadlineNS, 10, 64)
		if err != nil {
			t.Fatal(err)
		}
		deadline = &value
	}
	outcome := enforcement.Gate(context.Background(), &modelv1.ProducerEvent{Id: "fixture-event", Kind: vector.Event.Kind}, filter, deadline, deps, enforcement.Options{SourceHandle: "fixture-source", RequestID: vector.RequestID, IdempotencyKey: "fixture-idempotency"})
	if outcome.Kind.String() != vector.Expected.Kind {
		t.Errorf("kind = %s, want %s", outcome.Kind, vector.Expected.Kind)
	}
	if len(requests) != vector.Expected.DecideCalls {
		t.Fatalf("decide calls = %d, want %d", len(requests), vector.Expected.DecideCalls)
	}
	if readIndex != len(reads) {
		t.Errorf("clock reads = %d, want %d", readIndex, len(reads))
	}
	want := parseUint64(t, vector.Expected.RemainingTransportBudgetNS)
	if requests[0].GetRemainingTransportBudgetNanoseconds() != want {
		t.Errorf("remaining budget = %d, want %d", requests[0].GetRemainingTransportBudgetNanoseconds(), want)
	}
}

func deliverAnswer(t *testing.T, receiver *otlp.Receiver, answer []byte) {
	t.Helper()
	request := httptest.NewRequest(http.MethodPost, "/v1/logs", bytes.NewReader(answer))
	request.Header.Set("Content-Type", "application/x-protobuf")
	recorder := httptest.NewRecorder()
	receiver.ServeHTTP(recorder, request)
	if recorder.Code != http.StatusOK {
		t.Fatalf("deliver status = %d", recorder.Code)
	}
}

func parseUint64(t *testing.T, raw string) uint64 {
	t.Helper()
	value, err := strconv.ParseUint(raw, 10, 64)
	if err != nil {
		t.Fatal(err)
	}
	return value
}

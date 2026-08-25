package conformance

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"math/big"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"testing"

	modelv1 "github.com/emet-labs/sentinel/sdk/go/gen/sentinel/model/v1"
	probev1 "github.com/emet-labs/sentinel/sdk/go/gen/sentinel/probe/v1"
	"github.com/emet-labs/sentinel/sdk/go/enforcement"
	int128codec "github.com/emet-labs/sentinel/sdk/go/int128"
	"github.com/emet-labs/sentinel/sdk/go/internal/specmatch"
)

const version = "1.0.0"

type envelope struct {
	FormatVersion string          `json:"format_version"`
	Kind          string          `json:"kind"`
	Cases         json.RawMessage `json:"cases"`
}

type intCase struct {
	ID    string `json:"id"`
	Value string `json:"value"`
	High  string `json:"high"`
	Low   string `json:"low"`
}

type matchCase struct {
	ID                  string `json:"id"`
	SpecificationFilter struct {
		EventMatch *struct {
			EventKinds             []string `json:"event_kinds"`
			DeliveryMode          string   `json:"delivery_mode"`
			ProjectedAttributeKeys []string `json:"projected_attribute_keys"`
		} `json:"event_match"`
		FailMode string `json:"fail_mode"`
	} `json:"specification_filter"`
	ProducerEvent struct {
		Kind string `json:"kind"`
	} `json:"producer_event"`
	Expected bool `json:"expected"`
}

type gateCase struct {
	ID     string `json:"id"`
	Filter *struct {
		Epoch          string `json:"epoch"`
		Specifications []struct {
			ID               string   `json:"id"`
			EventKinds       []string `json:"event_kinds"`
			DeliveryMode    string   `json:"delivery_mode"`
			EvaluationMode  string   `json:"evaluation_mode"`
			Readiness       string   `json:"readiness"`
			LatencyBudgetNS string   `json:"latency_budget_ns"`
			FailMode        string   `json:"fail_mode"`
			AcceptedFailMode string  `json:"accepted_fail_mode"`
		} `json:"specifications"`
	} `json:"filter"`
	Event struct {
		Kind string `json:"kind"`
	} `json:"event"`
	LocalDeadlineNS *string  `json:"local_deadline_ns"`
	ClockReadsNS    []string `json:"clock_reads_ns"`
	Decider         struct {
		Result string `json:"result"`
	} `json:"decider"`
	Expected struct {
		Kind                       string  `json:"kind"`
		Reason                     *string `json:"reason"`
		DecideCalls                int     `json:"decide_calls"`
		RemainingTransportBudgetNS *string `json:"remaining_transport_budget_ns"`
	} `json:"expected"`
}

func fixtureRoot(t *testing.T) string {
	t.Helper()
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("runtime.Caller failed")
	}
	return filepath.Clean(filepath.Join(filepath.Dir(file), "..", "..", "..", "..", "testdata", "probe-sdk-conformance"))
}

func decodeStrict(t *testing.T, path string, out any) {
	t.Helper()
	data, err := os.ReadFile(filepath.Join(fixtureRoot(t), path))
	if err != nil {
		t.Fatal(err)
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(out); err != nil {
		t.Fatalf("%s: %v", path, err)
	}
	if decoder.More() {
		t.Fatalf("%s: trailing JSON", path)
	}
}

func unique(t *testing.T, ids []string) {
	t.Helper()
	seen := make(map[string]struct{}, len(ids))
	for _, id := range ids {
		if id == "" {
			t.Fatal("empty case id")
		}
		if _, exists := seen[id]; exists {
			t.Fatalf("duplicate case id %q", id)
		}
		seen[id] = struct{}{}
	}
}

func TestInt128Vectors(t *testing.T) {
	var suite struct {
		FormatVersion string    `json:"format_version"`
		Kind          string    `json:"kind"`
		Cases         []intCase `json:"cases"`
	}
	decodeStrict(t, "int128-v1.json", &suite)
	if suite.FormatVersion != version || suite.Kind != "int128" {
		t.Fatal("unsupported suite")
	}
	ids := make([]string, 0, len(suite.Cases))
	for _, vector := range suite.Cases {
		ids = append(ids, vector.ID)
		value, ok := new(big.Int).SetString(vector.Value, 10)
		if !ok {
			t.Fatalf("%s: invalid value", vector.ID)
		}
		high, ok := new(big.Int).SetString(vector.High, 10)
		if !ok || !high.IsInt64() {
			t.Fatalf("%s: invalid high", vector.ID)
		}
		low, ok := new(big.Int).SetString(vector.Low, 10)
		if !ok || low.Sign() < 0 || low.BitLen() > 64 {
			t.Fatalf("%s: invalid low", vector.ID)
		}
		encoded := int128codec.FromBigInt(value)
		if encoded.High != high.Int64() || encoded.Low != low.Uint64() {
			t.Errorf("%s: encoded words = (%d,%d)", vector.ID, encoded.High, encoded.Low)
		}
		decoded := int128codec.ToBigInt(&modelv1.Int128{High: high.Int64(), Low: low.Uint64()})
		if decoded.Cmp(value) != 0 {
			t.Errorf("%s: decoded = %s, want %s", vector.ID, decoded, value)
		}
	}
	unique(t, ids)
}

func TestManifestSuiteRegistryFailsClosed(t *testing.T) {
	var manifest struct {
		FormatVersion string `json:"format_version"`
		Suites        []struct {
			Kind string `json:"kind"`
			Path string `json:"path"`
		} `json:"suites"`
		Malformed []json.RawMessage `json:"malformed"`
	}
	decodeStrict(t, "manifest-v1.json", &manifest)
	want := []string{"spec_match:spec-match-v1.json", "int128:int128-v1.json", "enforcement_gate:enforcement-gate-v1.json"}
	if manifest.FormatVersion != version || len(manifest.Suites) != len(want) {
		t.Fatal("unsupported manifest")
	}
	for index, entry := range manifest.Suites {
		if entry.Kind+":"+entry.Path != want[index] {
			t.Fatalf("unknown manifest suite at %d", index)
		}
	}
}

func TestSpecMatchVectors(t *testing.T) {
	var suite struct {
		FormatVersion string      `json:"format_version"`
		Kind          string      `json:"kind"`
		Cases         []matchCase `json:"cases"`
	}
	decodeStrict(t, "spec-match-v1.json", &suite)
	if suite.FormatVersion != version || suite.Kind != "spec_match" {
		t.Fatal("unsupported suite")
	}
	ids := make([]string, 0, len(suite.Cases))
	for _, vector := range suite.Cases {
		ids = append(ids, vector.ID)
		spec := &modelv1.SpecificationFilter{}
		if vector.SpecificationFilter.EventMatch != nil {
			spec.EventMatch = &modelv1.EventMatch{
				EventKinds:             vector.SpecificationFilter.EventMatch.EventKinds,
				ProjectedAttributeKeys: vector.SpecificationFilter.EventMatch.ProjectedAttributeKeys,
			}
		}
		got := specmatch.Selects(spec, &modelv1.ProducerEvent{Kind: vector.ProducerEvent.Kind})
		if got != vector.Expected {
			t.Errorf("%s: Selects = %v, want %v", vector.ID, got, vector.Expected)
		}
	}
	unique(t, ids)
}

func TestEnforcementGateVectors(t *testing.T) {
	var suite struct {
		FormatVersion string     `json:"format_version"`
		Kind          string     `json:"kind"`
		Cases         []gateCase `json:"cases"`
	}
	decodeStrict(t, "enforcement-gate-v1.json", &suite)
	for _, vector := range suite.Cases {
		vector := vector
		t.Run(vector.ID, func(t *testing.T) {
			var filter *modelv1.EventFilter
			accepted := map[string]modelv1.FailMode{}
			if vector.Filter != nil {
				epoch, err := strconv.ParseUint(vector.Filter.Epoch, 10, 63)
				if err != nil { t.Fatal(err) }
				filter = &modelv1.EventFilter{Epoch: &epoch}
				for _, fixture := range vector.Filter.Specifications {
					failMode := modelv1.FailMode_FAIL_MODE_OPEN
					if fixture.FailMode == "closed" { failMode = modelv1.FailMode_FAIL_MODE_CLOSED }
					acceptedMode := modelv1.FailMode_FAIL_MODE_OPEN
					if fixture.AcceptedFailMode == "closed" { acceptedMode = modelv1.FailMode_FAIL_MODE_CLOSED }
					accepted[fixture.ID] = acceptedMode
					delivery := modelv1.DeliveryMode_DELIVERY_MODE_SHIP_ASYNC
					if fixture.DeliveryMode == "ask_and_block" { delivery = modelv1.DeliveryMode_DELIVERY_MODE_ASK_AND_BLOCK }
					budget, err := strconv.ParseUint(fixture.LatencyBudgetNS, 10, 64); if err != nil { t.Fatal(err) }
					filter.Specifications = append(filter.Specifications, &modelv1.SpecificationFilter{SpecificationId: fixture.ID, FailMode: failMode, EvaluationMode: modelv1.EvaluationMode_EVALUATION_MODE_ENFORCE, Readiness: modelv1.Readiness_READINESS_ACTIVE, LatencyBudgetNanoseconds: &budget, EventMatch: &modelv1.EventMatch{EventKinds: fixture.EventKinds, DeliveryMode: delivery}})
				}
			}
			reads := make([]int64, len(vector.ClockReadsNS))
			for index, raw := range vector.ClockReadsNS { value, err := strconv.ParseInt(raw, 10, 64); if err != nil { t.Fatal(err) }; reads[index] = value }
			readIndex := 0
			requests := make([]*probev1.DecideRequest, 0, 1)
			deps := enforcement.Deps{
				NowMonotonicNs: func() int64 { if readIndex >= len(reads) { t.Fatal("clock script exhausted") }; value := reads[readIndex]; readIndex++; return value },
				AcceptedFailModeFor: func(spec *modelv1.SpecificationFilter) modelv1.FailMode { return accepted[spec.GetSpecificationId()] },
				Decide: func(_ context.Context, request *probev1.DecideRequest) (*probev1.DecideResponse, error) {
					requests = append(requests, request)
					if vector.Decider.Result == "transport_error" { return nil, errors.New("fixture-transport-error") }
					actions := map[string]probev1.DecisionAction{"permit": probev1.DecisionAction_DECISION_ACTION_PERMIT, "deny": probev1.DecisionAction_DECISION_ACTION_DENY, "defer": probev1.DecisionAction_DECISION_ACTION_DEFER, "unspecified": probev1.DecisionAction_DECISION_ACTION_UNSPECIFIED}
					return &probev1.DecideResponse{Action: actions[vector.Decider.Result]}, nil
				},
			}
			var deadline *int64
			if vector.LocalDeadlineNS != nil { value, err := strconv.ParseInt(*vector.LocalDeadlineNS, 10, 64); if err != nil { t.Fatal(err) }; deadline = &value }
			outcome := enforcement.Gate(context.Background(), &modelv1.ProducerEvent{Id: "fixture-event", Kind: vector.Event.Kind}, filter, deadline, deps, enforcement.Options{SourceHandle: "fixture-source", RequestID: "fixture-request", IdempotencyKey: "fixture-idempotency"})
			if outcome.Kind.String() != vector.Expected.Kind { t.Errorf("kind = %s, want %s", outcome.Kind, vector.Expected.Kind) }
			if len(requests) != vector.Expected.DecideCalls { t.Errorf("decide calls = %d, want %d", len(requests), vector.Expected.DecideCalls) }
			if readIndex != len(reads) { t.Errorf("clock reads = %d, want %d", readIndex, len(reads)) }
			if len(requests) == 1 && vector.Expected.RemainingTransportBudgetNS != nil { want, _ := strconv.ParseUint(*vector.Expected.RemainingTransportBudgetNS, 10, 64); if requests[0].GetRemainingTransportBudgetNanoseconds() != want { t.Errorf("remaining budget mismatch") } }
		})
	}
}

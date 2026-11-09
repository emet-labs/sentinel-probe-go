package client

import (
	"net/http"
	"testing"

	modelv1 "github.com/emet-labs/sentinel/sdk/go/gen/sentinel/model/v1"
	probev1 "github.com/emet-labs/sentinel/sdk/go/gen/sentinel/probe/v1"
)

const testBaseURL = "http://sentinel.local:7070"

func newTestClient(t *testing.T, config Config) *ProbeClient {
	t.Helper()
	return New(config)
}

func makeEvent(id, kind string) *modelv1.ProducerEvent {
	return &modelv1.ProducerEvent{Id: id, Kind: kind, SchemaVersion: "sentinel.model.v1"}
}

func TestProbeClientHasNoFilterBeforeFirstRefresh(t *testing.T) {
	t.Parallel()

	client := newTestClient(t, Config{SourceHandle: "gateway.tool-calls", SentinelBaseURL: testBaseURL})
	if client.CurrentFilter() != nil {
		t.Fatal("CurrentFilter must be nil before the first refresh")
	}
	if client.AcknowledgedEpoch() != nil {
		t.Fatal("AcknowledgedEpoch must be nil before the first refresh")
	}
}

func TestProbeClientSetFilterSwapsAndStamps(t *testing.T) {
	t.Parallel()

	client := newTestClient(t, Config{SourceHandle: "gateway.tool-calls", SentinelBaseURL: testBaseURL})
	filter := makeFilter(u64(42))
	if !client.SetFilter(filter) {
		t.Fatal("first SetFilter must report an update")
	}
	if client.CurrentFilter() != filter {
		t.Fatal("CurrentFilter must return the exact pointer that was set")
	}
	if got := client.AcknowledgedEpoch(); got == nil || *got != 42 {
		t.Fatalf("AcknowledgedEpoch = %v, want 42", got)
	}
}

func TestProbeClientSetFilterSameEpochIsNoOp(t *testing.T) {
	t.Parallel()

	client := newTestClient(t, Config{SourceHandle: "gateway.tool-calls", SentinelBaseURL: testBaseURL})
	client.SetFilter(makeFilter(u64(42)))
	if client.SetFilter(makeFilter(u64(42))) {
		t.Fatal("re-pushing an equal epoch must be a no-op")
	}
}

func TestProbeClientRefreshOnEpoch(t *testing.T) {
	t.Parallel()

	client := newTestClient(t, Config{SourceHandle: "gateway.tool-calls", SentinelBaseURL: testBaseURL})
	client.SetFilter(makeFilter(u64(42)))
	if client.RefreshOnEpoch(u64(42)) {
		t.Fatal("an unchanged epoch must not trigger a refresh")
	}
	if !client.RefreshOnEpoch(u64(43)) {
		t.Fatal("a changed epoch must trigger a refresh")
	}
}

func TestProbeClientBuildDecideRequest(t *testing.T) {
	t.Parallel()

	client := newTestClient(t, Config{SourceHandle: "gateway.tool-calls", SentinelBaseURL: testBaseURL})
	client.SetFilter(makeFilter(u64(7)))
	event := makeEvent("evt-1", "approval.granted")

	request := client.BuildDecideRequest(event, "req-1", "idem-1", u64(5000))

	if request.GetRequestId() != "req-1" {
		t.Errorf("RequestId = %q", request.GetRequestId())
	}
	if request.GetIdempotencyKey() != "idem-1" {
		t.Errorf("IdempotencyKey = %q", request.GetIdempotencyKey())
	}
	if request.GetSourceHandle() != "gateway.tool-calls" {
		t.Errorf("SourceHandle = %q", request.GetSourceHandle())
	}
	if request.FilterEpoch == nil || *request.FilterEpoch != 7 {
		t.Errorf("FilterEpoch = %v, want 7", request.FilterEpoch)
	}
	if request.GetProducerEvent() != event {
		t.Error("ProducerEvent must be the exact event passed in, not a copy")
	}
	if request.RemainingTransportBudgetNanoseconds == nil ||
		*request.RemainingTransportBudgetNanoseconds != 5000 {
		t.Errorf("RemainingTransportBudgetNanoseconds = %v, want 5000",
			request.RemainingTransportBudgetNanoseconds)
	}
}

func TestProbeClientBuildDecideRequestOmitsAbsentBudget(t *testing.T) {
	t.Parallel()

	client := newTestClient(t, Config{SourceHandle: "gateway.tool-calls", SentinelBaseURL: testBaseURL})
	client.SetFilter(makeFilter(u64(7)))
	request := client.BuildDecideRequest(makeEvent("evt-1", "approval.granted"), "req-1", "idem-1", nil)
	if request.RemainingTransportBudgetNanoseconds != nil {
		t.Fatal("an absent budget must be absent on the wire, not zero")
	}
}

func TestProbeClientBuildDecideRequestOmitsEpochWhenNoFilterHeld(t *testing.T) {
	t.Parallel()

	client := newTestClient(t, Config{SourceHandle: "gateway.tool-calls", SentinelBaseURL: testBaseURL})
	request := client.BuildDecideRequest(makeEvent("evt-1", "approval.granted"), "req-1", "idem-1", u64(1000))
	if request.FilterEpoch != nil {
		t.Fatal("no filter held must leave filter_epoch absent")
	}
}

func TestProbeClientBuildDecideRequestCarriesEpochZero(t *testing.T) {
	t.Parallel()

	client := newTestClient(t, Config{SourceHandle: "gateway.tool-calls", SentinelBaseURL: testBaseURL})
	client.SetFilter(makeFilter(u64(0)))
	request := client.BuildDecideRequest(makeEvent("evt-1", "approval.granted"), "req-1", "idem-1", nil)
	if request.FilterEpoch == nil {
		t.Fatal("epoch 0 must be carried as present, not collapsed to absent")
	}
	if *request.FilterEpoch != 0 {
		t.Fatalf("FilterEpoch = %d, want 0", *request.FilterEpoch)
	}
}

func TestProbeClientInitialFilterSeedsStore(t *testing.T) {
	t.Parallel()

	filter := makeFilter(u64(99))
	client := newTestClient(t, Config{
		SourceHandle:    "gateway.tool-calls",
		SentinelBaseURL: testBaseURL,
		InitialFilter:   filter,
	})
	if client.CurrentFilter() != filter {
		t.Fatal("InitialFilter must seed the store")
	}
	if got := client.AcknowledgedEpoch(); got == nil || *got != 99 {
		t.Fatalf("AcknowledgedEpoch = %v, want 99", got)
	}
}

func TestProbeClientExposesSourceHandle(t *testing.T) {
	t.Parallel()

	client := New(Config{SourceHandle: "gateway.tool-calls"})
	if client.SourceHandle() != "gateway.tool-calls" {
		t.Fatalf("SourceHandle = %q", client.SourceHandle())
	}
}

// TestProbeClientDecideFunc reads a DecideResponse the caller already holds.
// It does not dial. The Connect adapter remains compiled and is covered by the
// loopback test; it is not this path.
func TestProbeClientDecideFunc(t *testing.T) {
	t.Parallel()

	probe := New(Config{SourceHandle: "gateway.tool-calls"})
	if _, err := probe.DecideFunc()(t.Context(), &probev1.DecideRequest{RequestId: "req-1"}); err == nil {
		t.Fatal("an ask before SetDecisions must fail")
	}
	probe.SetDecisions(stubDecisions{response: &probev1.DecideResponse{
		RequestId: "req-1",
		Action:    probev1.DecisionAction_DECISION_ACTION_DENY,
	}})
	response, err := probe.DecideFunc()(t.Context(), &probev1.DecideRequest{RequestId: "req-1"})
	if err != nil {
		t.Fatal(err)
	}
	if response.GetAction() != probev1.DecisionAction_DECISION_ACTION_DENY {
		t.Fatalf("action = %v", response.GetAction())
	}
}

type stubDecisions struct {
	response *probev1.DecideResponse
}

func (s stubDecisions) Response(requestID string) (*probev1.DecideResponse, error) {
	if s.response.GetRequestId() != requestID {
		return nil, errNoDecisionReceiver
	}
	return s.response, nil
}

func TestNewSentinelClientDefaultsHTTPClient(t *testing.T) {
	t.Parallel()

	// Two constructions, one defaulting and one explicit, must both yield a usable client.
	// HTTP/2 is deliberately not forced; http.DefaultClient speaks HTTP/1.1 and Connect
	// works over both.
	if NewSentinelClient(TransportOptions{BaseURL: testBaseURL}) == nil {
		t.Fatal("a nil HTTPClient must default rather than produce a nil client")
	}
	if NewSentinelClient(TransportOptions{BaseURL: testBaseURL, HTTPClient: &http.Client{}}) == nil {
		t.Fatal("an explicit HTTPClient must be honoured")
	}
}

package enforcement_test

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"connectrpc.com/connect"

	"github.com/emet-labs/sentinel-probe-go/client"
	"github.com/emet-labs/sentinel-probe-go/enforcement"
	probev1 "github.com/emet-labs/sentinel-probe-go/gen/sentinel/probe/v1"
	"github.com/emet-labs/sentinel-probe-go/gen/sentinel/probe/v1/probev1connect"
)

// A capability the TypeScript reference declined as out of scope, and about thirty lines of
// stdlib here: stand the generated Connect handler up on httptest and drive the real
// generated client through it. This exercises the actual proto wire encoding, the HTTP
// transport and Connect's error mapping, all of which the in-process mock decider skips —
// and still with no external dependency and no network beyond loopback.

// loopbackService is a scripted SentinelDecisionService handler.
type loopbackService struct {
	action   probev1.DecisionAction
	err      error
	requests chan *probev1.DecideRequest
}

func (s *loopbackService) Decide(
	_ context.Context,
	request *connect.Request[probev1.DecideRequest],
) (*connect.Response[probev1.DecideResponse], error) {
	select {
	case s.requests <- request.Msg:
	default:
	}
	if s.err != nil {
		return nil, s.err
	}
	return connect.NewResponse(&probev1.DecideResponse{
		RequestId: request.Msg.GetRequestId(),
		Action:    s.action,
	}), nil
}

func startLoopback(t *testing.T, service *loopbackService) probev1connect.SentinelDecisionServiceClient {
	t.Helper()
	service.requests = make(chan *probev1.DecideRequest, 4)

	mux := http.NewServeMux()
	mux.Handle(probev1connect.NewSentinelDecisionServiceHandler(service))
	server := httptest.NewServer(mux)
	t.Cleanup(server.Close)

	return client.NewSentinelClient(client.TransportOptions{
		BaseURL:    server.URL,
		HTTPClient: server.Client(),
	})
}

// deciderFor is client.DecideFunc, the adapter the SDK ships so a host does not have to write
// the Connect envelope wrapping by hand. Driving the shipped adapter here rather than a local
// copy is the point: this exercises the production path end to end.
func deciderFor(decider probev1connect.SentinelDecisionServiceClient) func(
	context.Context, *probev1.DecideRequest,
) (*probev1.DecideResponse, error) {
	return client.DecideFunc(decider)
}

func TestConnectLoopbackDeny(t *testing.T) {
	t.Parallel()

	service := &loopbackService{action: probev1.DecisionAction_DECISION_ACTION_DENY}
	decider := startLoopback(t, service)

	event := makeEvent(testKind)
	outcome := enforcement.Gate(t.Context(), event, makeFilter(u64(testEpoch), askAndBlockSpec()),
		i64(10000), enforcement.Deps{
			Decide:              deciderFor(decider),
			NowMonotonicNs:      func() int64 { return 0 },
			AcceptedFailModeFor: alwaysOpen,
		}, testOptions)

	if outcome.Kind != enforcement.OutcomeDeny {
		t.Fatalf("Kind = %v, want deny over the real wire", outcome.Kind)
	}

	// The request survived proto encoding, HTTP transport and decoding intact.
	received := <-service.requests
	if received.GetRequestId() != "req-1" || received.GetIdempotencyKey() != "idem-1" {
		t.Fatalf("identifiers lost on the wire: %v", received)
	}
	if received.GetProducerEvent().GetKind() != testKind {
		t.Fatalf("producer event lost on the wire: %v", received.GetProducerEvent())
	}
	if received.FilterEpoch == nil || *received.FilterEpoch != testEpoch {
		t.Fatalf("filter epoch lost on the wire: %v", received.FilterEpoch)
	}
	if received.RemainingTransportBudgetNanoseconds == nil ||
		*received.RemainingTransportBudgetNanoseconds != 10000 {
		t.Fatalf("budget lost on the wire: %v", received.RemainingTransportBudgetNanoseconds)
	}
}

func TestConnectLoopbackPermit(t *testing.T) {
	t.Parallel()

	decider := startLoopback(t, &loopbackService{action: probev1.DecisionAction_DECISION_ACTION_PERMIT})
	outcome := enforcement.Gate(t.Context(), makeEvent(testKind),
		makeFilter(u64(testEpoch), askAndBlockSpec()), i64(10000), enforcement.Deps{
			Decide:              deciderFor(decider),
			NowMonotonicNs:      func() int64 { return 0 },
			AcceptedFailModeFor: alwaysOpen,
		}, testOptions)

	if outcome.Kind != enforcement.OutcomePermit {
		t.Fatalf("Kind = %v, want permit", outcome.Kind)
	}
}

// TestConnectLoopbackServerErrorFailsClosed exercises Connect's error mapping end to end: the
// handler returns a coded error, the client surfaces a *connect.Error, and the gate routes it
// into the contracted fail mode with the code recorded for audit.
func TestConnectLoopbackServerErrorFailsClosed(t *testing.T) {
	t.Parallel()

	decider := startLoopback(t, &loopbackService{
		err: connect.NewError(connect.CodeUnavailable, errors.New("sentinel unavailable")),
	})
	outcome := enforcement.Gate(t.Context(), makeEvent(testKind),
		makeFilter(u64(testEpoch), closedAskAndBlockSpec()), i64(10000), enforcement.Deps{
			Decide:              deciderFor(decider),
			NowMonotonicNs:      func() int64 { return 0 },
			AcceptedFailModeFor: alwaysClosed,
		}, testOptions)

	if outcome.Kind != enforcement.OutcomeFailClosedDeny {
		t.Fatalf("Kind = %v, want fail-closed-deny", outcome.Kind)
	}
	if outcome.Reason[:len("connect-unavailable")] != "connect-unavailable" {
		t.Fatalf("Reason = %q, want the Connect code preserved across the wire", outcome.Reason)
	}
}

// TestConnectLoopbackUnreachableEndpointFailsOpen: nothing is listening, so the client's own
// transport error reaches the gate.
func TestConnectLoopbackUnreachableEndpointFailsOpen(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.NewServeMux())
	url := server.URL
	server.Close() // nothing is listening any more

	decider := client.NewSentinelClient(client.TransportOptions{BaseURL: url})
	outcome := enforcement.Gate(t.Context(), makeEvent(testKind),
		makeFilter(u64(testEpoch), askAndBlockSpec()), i64(10000), enforcement.Deps{
			Decide:              deciderFor(decider),
			NowMonotonicNs:      func() int64 { return 0 },
			AcceptedFailModeFor: alwaysOpen,
		}, testOptions)

	if outcome.Kind != enforcement.OutcomeFailOpenPermit {
		t.Fatalf("Kind = %v, want fail-open-permit", outcome.Kind)
	}
	if outcome.Reason == "" {
		t.Fatal("a transport failure must carry a reason")
	}
}

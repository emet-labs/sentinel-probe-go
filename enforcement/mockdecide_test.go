package enforcement_test

import (
	"context"
	"sync"

	probev1 "github.com/emet-labs/sentinel/sdk/go/gen/sentinel/probe/v1"
)

// mockDecider is the in-process Decide stub, mirroring
// tests/enforcement/mock-decide-server.ts: a canned response, a scripted sequence whose last
// element repeats, or a forced error, while recording every request and the call count.
//
// Deliberately not an HTTP server: the gate calls the dependency function directly, and
// connect_loopback_test.go covers the real wire separately.
type mockDecider struct {
	response *probev1.DecideResponse
	sequence []*probev1.DecideResponse
	err      error

	mu        sync.Mutex
	requests  []*probev1.DecideRequest
	nextIndex int
}

func (m *mockDecider) decide(_ context.Context, request *probev1.DecideRequest) (*probev1.DecideResponse, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.requests = append(m.requests, request)

	if m.err != nil {
		return nil, m.err
	}
	if len(m.sequence) > 0 {
		response := m.sequence[m.nextIndex]
		if m.nextIndex < len(m.sequence)-1 {
			m.nextIndex++
		}
		return response, nil
	}
	if m.response != nil {
		return m.response, nil
	}
	return &probev1.DecideResponse{
		RequestId: request.GetRequestId(),
		Action:    probev1.DecisionAction_DECISION_ACTION_PERMIT,
	}, nil
}

func (m *mockDecider) callCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.requests)
}

func (m *mockDecider) lastRequest() *probev1.DecideRequest {
	m.mu.Lock()
	defer m.mu.Unlock()
	if len(m.requests) == 0 {
		return nil
	}
	return m.requests[len(m.requests)-1]
}

// makeResponse builds a canned DecideResponse, optionally with per-spec decisions.
func makeResponse(action probev1.DecisionAction, decisions ...*probev1.SpecificationDecision) *probev1.DecideResponse {
	return &probev1.DecideResponse{RequestId: "mock", Action: action, Specifications: decisions}
}

func makeDecision(
	specificationID string,
	action probev1.DecisionAction,
	unresolved *probev1.UnresolvedReason,
) *probev1.SpecificationDecision {
	return &probev1.SpecificationDecision{
		SpecificationId:      specificationID,
		SpecificationVersion: "1.0.0",
		Action:               action,
		UnresolvedReason:     unresolved,
	}
}

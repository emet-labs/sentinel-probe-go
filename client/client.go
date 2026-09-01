package client

import (
	"context"

	"connectrpc.com/connect"

	modelv1 "github.com/emet-labs/sentinel-probe-go/gen/sentinel/model/v1"
	probev1 "github.com/emet-labs/sentinel-probe-go/gen/sentinel/probe/v1"
	"github.com/emet-labs/sentinel-probe-go/gen/sentinel/probe/v1/probev1connect"
)

// Config describes the Probe's identity and the Sentinel it talks to.
// Analog of probe-client.ts's ProbeClientConfig.
type Config struct {
	// SourceHandle is the Probe's source_handle, used in DecideRequest.source_handle.
	SourceHandle string
	// SentinelBaseURL is the Sentinel decision endpoint base URL. The SDK never reads it —
	// the decider passed to New is what carries requests, and it already holds its own base
	// URL. The field exists for parity with the reference's ProbeClientConfig.sentinelBaseUrl,
	// which is equally inert there, and so a host can keep its endpoint configuration in one
	// struct.
	SentinelBaseURL string
	// InitialFilter optionally seeds the store before the first push, for example from a
	// local cache.
	InitialFilter *modelv1.EventFilter
}

// ProbeClient owns the Probe's filter state and builds decision requests against it.
// Analog of probe-client.ts's ProbeClient.
//
// Safe for concurrent use: all mutable state lives behind FilterStore's atomic pointer.
type ProbeClient struct {
	config  Config
	store   *FilterStore
	decider probev1connect.SentinelDecisionServiceClient
}

// New builds a ProbeClient. The decider is a REQUIRED parameter with no default: the
// TypeScript reference originally defaulted its transport, which was corrected on review
// because a silently-defaulted transport hides a misconfigured endpoint until the first
// enforcement call. Pass client.NewSentinelClient(...) in production and a stub in tests.
func New(config Config, decider probev1connect.SentinelDecisionServiceClient) *ProbeClient {
	return &ProbeClient{
		config:  config,
		store:   NewFilterStore(config.InitialFilter),
		decider: decider,
	}
}

// CurrentFilter returns the EventFilter for this source, or nil before the first refresh.
// The pointer is stable until the next SetFilter.
func (c *ProbeClient) CurrentFilter() *modelv1.EventFilter {
	return c.store.Get()
}

// AcknowledgedEpoch returns the held filter epoch, the value a Probe stamps into
// ProducerEvent.acknowledged_filter_epoch. nil means no epoch is held; it does not mean 0.
func (c *ProbeClient) AcknowledgedEpoch() *uint64 {
	return c.store.Epoch()
}

// SetFilter swaps in a new EventFilter, reporting whether the store was actually updated.
//
// Where the new filter comes from is out of SDK scope: in v1 Sentinel pushes filters
// (ADR-0006) and no push RPC exists in decision.proto, so the host calls SetFilter when a
// push arrives. Same division of labour as the TypeScript reference.
func (c *ProbeClient) SetFilter(filter *modelv1.EventFilter) bool {
	return c.store.Set(filter)
}

// RefreshOnEpoch reports whether an announced epoch warrants fetching a new filter.
func (c *ProbeClient) RefreshOnEpoch(newEpoch *uint64) bool {
	return c.store.ShouldRefresh(newEpoch)
}

// BuildDecideRequest builds a DecideRequest against the held filter epoch.
//
// The event must already have been projected by filter.ApplyFilter; this method does not
// project, exactly as the TypeScript reference does not. remainingBudgetNs is nil when the
// caller set no latency budget.
func (c *ProbeClient) BuildDecideRequest(
	event *modelv1.ProducerEvent,
	requestID string,
	idempotencyKey string,
	remainingBudgetNs *uint64,
) *probev1.DecideRequest {
	return &probev1.DecideRequest{
		RequestId:                           requestID,
		IdempotencyKey:                      idempotencyKey,
		SourceHandle:                        c.config.SourceHandle,
		FilterEpoch:                         c.store.Epoch(),
		ProducerEvent:                       event,
		RemainingTransportBudgetNanoseconds: remainingBudgetNs,
	}
}

// Decider exposes the decision client so the enforcement gate can call it.
func (c *ProbeClient) Decider() probev1connect.SentinelDecisionServiceClient {
	return c.decider
}

// SourceHandle returns the configured source handle, which the enforcement gate stamps into
// every DecideRequest.
func (c *ProbeClient) SourceHandle() string {
	return c.config.SourceHandle
}

// DecideFunc adapts this client's decider to the shape the enforcement gate's Decide
// dependency expects, so a host does not have to write the Connect envelope wrapping by hand:
//
//	outcome := enforcement.Gate(ctx, projected, probe.CurrentFilter(), deadline,
//	    enforcement.Deps{Decide: probe.DecideFunc(), NowMonotonicNs: ..., AcceptedFailModeFor: ...},
//	    enforcement.Options{SourceHandle: probe.SourceHandle(), ...})
//
// The enforcement package deliberately takes a plain function rather than the generated
// interface, so it stays testable without a transport; this is the adapter between the two.
// The reference has the same gap around getTransport() and never closed it.
func (c *ProbeClient) DecideFunc() func(context.Context, *probev1.DecideRequest) (*probev1.DecideResponse, error) {
	return DecideFunc(c.decider)
}

// DecideFunc adapts any SentinelDecisionServiceClient to the enforcement gate's Decide
// dependency. Use the ProbeClient method unless the decider is not the one a ProbeClient
// holds. Errors are returned unwrapped so the gate can classify *connect.Error and context
// errors distinctly.
func DecideFunc(
	decider probev1connect.SentinelDecisionServiceClient,
) func(context.Context, *probev1.DecideRequest) (*probev1.DecideResponse, error) {
	return func(ctx context.Context, request *probev1.DecideRequest) (*probev1.DecideResponse, error) {
		response, err := decider.Decide(ctx, connect.NewRequest(request))
		if err != nil {
			return nil, err
		}
		return response.Msg, nil
	}
}

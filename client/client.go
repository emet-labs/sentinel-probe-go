package client

import (
	"context"
	"errors"
	"math"
	"time"

	"connectrpc.com/connect"

	modelv1 "github.com/emet-labs/sentinel-probe-go/gen/sentinel/model/v1"
	probev1 "github.com/emet-labs/sentinel-probe-go/gen/sentinel/probe/v1"
	"github.com/emet-labs/sentinel-probe-go/gen/sentinel/probe/v1/probev1connect"
)

// Decisions supplies a DecideResponse decoded from an OTLP log. The live path
// reads this. It does not dial SentinelDecisionService.
type Decisions interface {
	Response(requestID string) (*probev1.DecideResponse, error)
}

// DecisionWaiter is a Decisions source that can wait for a DecideResponse that
// has not arrived yet. otlp.Receiver implements it. DecideFunc uses Await when
// the installed Decisions implements it, and the non-blocking Response
// otherwise, so an existing Decisions implementation keeps compiling.
type DecisionWaiter interface {
	Decisions
	// Await returns the DecideResponse for requestID, waiting up to budget for
	// it. A budget of zero arms no wait.
	Await(ctx context.Context, requestID string, budget time.Duration) (*probev1.DecideResponse, error)
}

// Config describes the Probe's identity and the Sentinel it talks to.
// Analog of probe-client.ts's ProbeClientConfig.
type Config struct {
	// SourceHandle is the Probe's source_handle, used in DecideRequest.source_handle.
	SourceHandle string
	// SentinelBaseURL is the OTLP base URL the host passes to the tracer. This
	// package does not dial it. Decide responses arrive on the log receiver.
	SentinelBaseURL string
	// InitialFilter optionally seeds the store before the first push, for example from a
	// local cache.
	InitialFilter *modelv1.EventFilter
}

// ProbeClient owns the Probe's filter state and builds decision requests against it.
// Analog of probe-client.ts's ProbeClient.
//
// Safe for concurrent use of the filter store. SetDecisions must happen before
// the gate asks, or the caller must synchronize it with DecideFunc.
type ProbeClient struct {
	config    Config
	store     *FilterStore
	decisions Decisions
}

// New builds a ProbeClient. Decisions arrive later, through SetDecisions, from
// the OTLP log receiver. There is no Connect client on this path.
func New(config Config) *ProbeClient {
	return &ProbeClient{
		config: config,
		store:  NewFilterStore(config.InitialFilter),
	}
}

// SetDecisions installs the OTLP log source the gate reads.
func (c *ProbeClient) SetDecisions(decisions Decisions) {
	c.decisions = decisions
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
// Only a first filter or a strictly newer epoch updates it; an equal epoch is a no-op and an
// older or epoch-less filter is refused (see FilterStore.Set).
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

// SourceHandle returns the configured source handle, which the enforcement gate stamps into
// every DecideRequest.
func (c *ProbeClient) SourceHandle() string {
	return c.config.SourceHandle
}

// DecideFunc adapts the OTLP log receiver to the enforcement gate. When the
// installed Decisions is a DecisionWaiter, the ask waits for the matching
// DecideResponse log for the request's remaining transport budget (ADR-0024),
// capped by ctx. An absent or zero budget does not wait. A missing receiver, a
// missing response, or an expired wait is an error, and the gate applies the
// fail mode. This function does not dial SentinelDecisionService.
func (c *ProbeClient) DecideFunc() func(context.Context, *probev1.DecideRequest) (*probev1.DecideResponse, error) {
	return func(ctx context.Context, request *probev1.DecideRequest) (*probev1.DecideResponse, error) {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if c.decisions == nil {
			return nil, errNoDecisionReceiver
		}
		waiter, ok := c.decisions.(DecisionWaiter)
		if !ok {
			return c.decisions.Response(request.GetRequestId())
		}
		return waiter.Await(ctx, request.GetRequestId(), waitBudget(request))
	}
}

// waitBudget is the request's remaining transport budget as a Duration. Absent
// is zero, and a budget beyond time.Duration's range is capped at its maximum.
func waitBudget(request *probev1.DecideRequest) time.Duration {
	budget := request.RemainingTransportBudgetNanoseconds
	if budget == nil {
		return 0
	}
	if *budget > math.MaxInt64 {
		return time.Duration(math.MaxInt64)
	}
	return time.Duration(*budget)
}

// errNoDecisionReceiver is returned when the gate asks before SetDecisions.
var errNoDecisionReceiver = errors.New("otlp: DecideResponse receiver is not configured")

// DecideFunc adapts a Connect client to the gate's Decide dependency. It is not
// the live path: #398 carries DecideResponse on an OTLP log, and the Decide RPC
// is not served. The loopback test is the only caller. Errors are returned
// unwrapped so the gate can classify *connect.Error and context errors.
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

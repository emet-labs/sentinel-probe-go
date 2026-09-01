package gensmoke_test

import (
	"testing"

	"google.golang.org/protobuf/proto"

	modelv1 "github.com/emet-labs/sentinel-probe-go/gen/sentinel/model/v1"
	probev1 "github.com/emet-labs/sentinel-probe-go/gen/sentinel/probe/v1"
	"github.com/emet-labs/sentinel-probe-go/gen/sentinel/probe/v1/probev1connect"
)

// TestGeneratedMessagesRoundTrip proves the generated messages construct and survive a
// binary round-trip, i.e. the codegen output is a working protobuf runtime type and the
// go_package_prefix override resolved model/v1 <- probe/v1 cross-imports correctly.
func TestGeneratedMessagesRoundTrip(t *testing.T) {
	t.Parallel()

	epoch := uint64(7)
	req := &probev1.DecideRequest{
		RequestId:      "req-1",
		IdempotencyKey: "idem-1",
		SourceHandle:   "checkout",
		FilterEpoch:    &epoch,
		ProducerEvent: &modelv1.ProducerEvent{
			Id:                 "event-1",
			Kind:               "order.charged",
			SchemaVersion:      "v1",
			ClaimedSensitivity: modelv1.Sensitivity_SENSITIVITY_INTERNAL,
			Attributes: []*modelv1.AttributeEntry{
				{Key: "sagashop.order.id", Value: &modelv1.AttributeValue{
					Value: &modelv1.AttributeValue_StringValue{StringValue: "ord-9"},
				}},
			},
			CausalPredecessorIds: []string{"event-0"},
		},
	}

	wire, err := proto.Marshal(req)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var got probev1.DecideRequest
	if err := proto.Unmarshal(wire, &got); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if !proto.Equal(req, &got) {
		t.Fatalf("round-trip mismatch:\n want %v\n  got %v", req, &got)
	}
	if got.GetProducerEvent().GetAttributes()[0].GetValue().GetStringValue() != "ord-9" {
		t.Fatalf("nested attribute lost: %v", got.GetProducerEvent().GetAttributes())
	}
}

// TestConnectSurface pins the Connect client/handler identifiers the client and enforcement
// packages import. protoc-gen-connect-go derives its package name from protoc-gen-go's
// GoPackageName; with buf managed mode that resolves to probev1connect (not v1connect), and
// the whole SDK's import convention depends on that.
func TestConnectSurface(t *testing.T) {
	t.Parallel()

	var client probev1connect.SentinelDecisionServiceClient
	if client != nil {
		t.Fatal("zero-value interface should be nil")
	}
	var handler probev1connect.SentinelDecisionServiceHandler = probev1connect.UnimplementedSentinelDecisionServiceHandler{}
	if handler == nil {
		t.Fatal("UnimplementedSentinelDecisionServiceHandler must satisfy the handler interface")
	}
	if probev1connect.SentinelDecisionServiceName != "sentinel.probe.v1.SentinelDecisionService" {
		t.Fatalf("unexpected service name %q", probev1connect.SentinelDecisionServiceName)
	}
	if probev1connect.SentinelDecisionServiceDecideProcedure != "/sentinel.probe.v1.SentinelDecisionService/Decide" {
		t.Fatalf("unexpected procedure %q", probev1connect.SentinelDecisionServiceDecideProcedure)
	}
}

// TestEnumConstantsKeepProtoPrefix pins D5. bufbuild/es strips enum prefixes (FailMode.CLOSED);
// protoc-gen-go does not. Transliterating the TypeScript form would not compile, and a silent
// renumbering would change the wire contract, so both name and number are asserted.
func TestEnumConstantsKeepProtoPrefix(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name string
		got  int32
		want int32
	}{
		{"FailMode_FAIL_MODE_UNSPECIFIED", int32(modelv1.FailMode_FAIL_MODE_UNSPECIFIED), 0},
		{"FailMode_FAIL_MODE_OPEN", int32(modelv1.FailMode_FAIL_MODE_OPEN), 1},
		{"FailMode_FAIL_MODE_CLOSED", int32(modelv1.FailMode_FAIL_MODE_CLOSED), 2},
		{"DeliveryMode_DELIVERY_MODE_SHIP_ASYNC", int32(modelv1.DeliveryMode_DELIVERY_MODE_SHIP_ASYNC), 1},
		{"DeliveryMode_DELIVERY_MODE_ASK_AND_BLOCK", int32(modelv1.DeliveryMode_DELIVERY_MODE_ASK_AND_BLOCK), 2},
		{"SourceTier_SOURCE_TIER_ANCHOR", int32(modelv1.SourceTier_SOURCE_TIER_ANCHOR), 1},
		{"SourceTier_SOURCE_TIER_CONTRIBUTING", int32(modelv1.SourceTier_SOURCE_TIER_CONTRIBUTING), 2},
		{"Sensitivity_SENSITIVITY_UNSPECIFIED", int32(modelv1.Sensitivity_SENSITIVITY_UNSPECIFIED), 0},
		{"SourceCapability_..._BOUNDED_CLOCK_UNCERTAINTY", int32(modelv1.SourceCapability_SOURCE_CAPABILITY_BOUNDED_CLOCK_UNCERTAINTY), 4},
		{"DecisionAction_DECISION_ACTION_UNSPECIFIED", int32(probev1.DecisionAction_DECISION_ACTION_UNSPECIFIED), 0},
		{"DecisionAction_DECISION_ACTION_PERMIT", int32(probev1.DecisionAction_DECISION_ACTION_PERMIT), 1},
		{"DecisionAction_DECISION_ACTION_DENY", int32(probev1.DecisionAction_DECISION_ACTION_DENY), 2},
		{"DecisionAction_DECISION_ACTION_DEFER", int32(probev1.DecisionAction_DECISION_ACTION_DEFER), 3},
		{"UnresolvedReason_UNRESOLVED_REASON_EVIDENCE_GAP", int32(probev1.UnresolvedReason_UNRESOLVED_REASON_EVIDENCE_GAP), 4},
		{"UnresolvedReason_UNRESOLVED_REASON_TIMEOUT", int32(probev1.UnresolvedReason_UNRESOLVED_REASON_TIMEOUT), 5},
	}
	for _, tc := range cases {
		if tc.got != tc.want {
			t.Errorf("%s = %d, want %d", tc.name, tc.got, tc.want)
		}
	}
}

// TestOptionalEpochPresenceIsPointerShaped pins the epoch-0 trap at its source. EventFilter.epoch
// is `optional uint64`, so protoc-gen-go emits *uint64 plus a nil-safe GetEpoch() that returns 0
// for nil — and 0 is a legitimate epoch. Every presence check in this SDK must be `Epoch == nil`,
// never `GetEpoch() == 0`. The TypeScript port got this free from `bigint | undefined`.
func TestOptionalEpochPresenceIsPointerShaped(t *testing.T) {
	t.Parallel()

	absent := &modelv1.EventFilter{}
	if absent.Epoch != nil {
		t.Fatal("unset epoch must be a nil pointer")
	}
	if absent.GetEpoch() != 0 {
		t.Fatal("GetEpoch on an absent epoch must return the zero value")
	}

	zero := uint64(0)
	present := &modelv1.EventFilter{Epoch: &zero}
	if present.Epoch == nil {
		t.Fatal("epoch 0 must be present, not nil")
	}
	if present.GetEpoch() != absent.GetEpoch() {
		t.Fatal("GetEpoch cannot distinguish epoch 0 from absent — that is the trap")
	}

	// The wire form does distinguish them, which is why the pointer check is the correct one.
	absentWire, err := proto.Marshal(absent)
	if err != nil {
		t.Fatalf("marshal absent: %v", err)
	}
	presentWire, err := proto.Marshal(present)
	if err != nil {
		t.Fatalf("marshal present: %v", err)
	}
	if len(absentWire) == len(presentWire) {
		t.Fatalf("epoch 0 must be encoded on the wire; absent=%d bytes present=%d bytes",
			len(absentWire), len(presentWire))
	}
}

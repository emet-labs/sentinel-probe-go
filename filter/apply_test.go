package filter_test

import (
	"slices"
	"testing"

	"github.com/emet-labs/sentinel-probe-go/filter"
	modelv1 "github.com/emet-labs/sentinel-probe-go/gen/sentinel/model/v1"
)

func attr(key, value string) *modelv1.AttributeEntry {
	return &modelv1.AttributeEntry{
		Key:   key,
		Value: &modelv1.AttributeValue{Value: &modelv1.AttributeValue_StringValue{StringValue: value}},
	}
}

func event(kind string, attrs []*modelv1.AttributeEntry, predecessors ...string) *modelv1.ProducerEvent {
	return &modelv1.ProducerEvent{
		Id:                   "test-event",
		Kind:                 kind,
		SchemaVersion:        "sentinel.model.v1",
		Attributes:           attrs,
		CausalPredecessorIds: predecessors,
	}
}

func spec(kinds, keys []string) *modelv1.SpecificationFilter {
	return &modelv1.SpecificationFilter{
		SpecificationId:      "spec-1",
		SpecificationVersion: "1.0.0",
		EventMatch: &modelv1.EventMatch{
			EventKinds:             kinds,
			ProjectedAttributeKeys: keys,
			DeliveryMode:           modelv1.DeliveryMode_DELIVERY_MODE_SHIP_ASYNC,
		},
	}
}

func eventFilter(specs ...*modelv1.SpecificationFilter) *modelv1.EventFilter {
	epoch := uint64(5)
	return &modelv1.EventFilter{Epoch: &epoch, Specifications: specs}
}

func keysOf(event *modelv1.ProducerEvent) []string {
	keys := make([]string, 0, len(event.GetAttributes()))
	for _, entry := range event.GetAttributes() {
		keys = append(keys, entry.GetKey())
	}
	return keys
}

func TestApplyFilterNoSpecSelectsDrops(t *testing.T) {
	t.Parallel()

	got := filter.ApplyFilter(
		event("transfer.initiated", []*modelv1.AttributeEntry{attr("amount", "100")}),
		eventFilter(spec([]string{"approval.granted"}, []string{"approver"})),
	)
	if got != nil {
		t.Fatalf("kind mismatch must drop the event, got %v", got)
	}
}

func TestApplyFilterProjectsSubset(t *testing.T) {
	t.Parallel()

	got := filter.ApplyFilter(
		event("approval.granted", []*modelv1.AttributeEntry{
			attr("approver", "alice"), attr("binding_id", "b-1"), attr("amount", "1000"),
		}),
		eventFilter(spec([]string{"approval.granted"}, []string{"approver", "binding_id"})),
	)
	if got == nil {
		t.Fatal("a selecting spec must not drop the event")
	}
	keys := keysOf(got)
	if len(keys) != 2 || !slices.Contains(keys, "approver") || !slices.Contains(keys, "binding_id") {
		t.Fatalf("keys = %v, want exactly approver and binding_id", keys)
	}
	if slices.Contains(keys, "amount") {
		t.Fatal("an unprojected attribute must be trimmed")
	}
}

func TestApplyFilterEmptyProjectionKeepsAll(t *testing.T) {
	t.Parallel()

	got := filter.ApplyFilter(
		event("approval.granted", []*modelv1.AttributeEntry{attr("approver", "alice"), attr("amount", "1000")}),
		eventFilter(spec([]string{"approval.granted"}, nil)),
	)
	if got == nil || len(got.GetAttributes()) != 2 {
		t.Fatalf("an empty projection set must keep every attribute, got %v", got)
	}
}

func TestApplyFilterUnionsProjectedKeys(t *testing.T) {
	t.Parallel()

	got := filter.ApplyFilter(
		event("transfer.initiated", []*modelv1.AttributeEntry{
			attr("amount", "100"), attr("account", "acc-1"), attr("other", "x"),
		}),
		eventFilter(
			spec([]string{"transfer.initiated"}, []string{"amount"}),
			spec(nil, []string{"account"}), // empty event_kinds matches every kind
		),
	)
	if got == nil {
		t.Fatal("event must survive")
	}
	keys := keysOf(got)
	if !slices.Contains(keys, "amount") || !slices.Contains(keys, "account") {
		t.Fatalf("keys = %v, want the union of both projections", keys)
	}
	if slices.Contains(keys, "other") {
		t.Fatal("a key outside the union must be trimmed")
	}
}

func TestApplyFilterProjectAllWinsOverSubset(t *testing.T) {
	t.Parallel()

	got := filter.ApplyFilter(
		event("x", []*modelv1.AttributeEntry{attr("a", "1"), attr("b", "2")}),
		eventFilter(spec([]string{"x"}, nil), spec([]string{"x"}, []string{"a"})),
	)
	if got == nil || len(got.GetAttributes()) != 2 {
		t.Fatalf("one project-all spec must over-approximate upward, got %v", got)
	}
}

func TestApplyFilterEmptyFilterDrops(t *testing.T) {
	t.Parallel()

	if got := filter.ApplyFilter(event("anything", []*modelv1.AttributeEntry{attr("k", "v")}), eventFilter()); got != nil {
		t.Fatalf("a filter with no specifications must drop, got %v", got)
	}
}

func TestApplyFilterNilFilterDrops(t *testing.T) {
	t.Parallel()

	if got := filter.ApplyFilter(event("anything", nil), nil); got != nil {
		t.Fatalf("a nil filter must behave as a filter with no specifications, got %v", got)
	}
}

func TestApplyFilterEmptyEventKindsMatchesAll(t *testing.T) {
	t.Parallel()

	got := filter.ApplyFilter(
		event("anything", []*modelv1.AttributeEntry{attr("k", "v"), attr("drop", "x")}),
		eventFilter(spec(nil, []string{"k"})),
	)
	if got == nil || len(got.GetAttributes()) != 1 || got.GetAttributes()[0].GetKey() != "k" {
		t.Fatalf("empty event_kinds must match every kind and still project, got %v", got)
	}
}

func TestApplyFilterNeverTrimsCausalPredecessors(t *testing.T) {
	t.Parallel()

	got := filter.ApplyFilter(
		event("x", []*modelv1.AttributeEntry{attr("keep", "v")}, "pred-a", "pred-b"),
		eventFilter(spec([]string{"x"}, []string{"keep"})),
	)
	if got == nil {
		t.Fatal("event must survive")
	}
	if !slices.Equal(got.GetCausalPredecessorIds(), []string{"pred-a", "pred-b"}) {
		t.Fatalf("causal predecessors = %v, want them untouched", got.GetCausalPredecessorIds())
	}
}

func TestApplyFilterDropsUnprojectedAttribute(t *testing.T) {
	t.Parallel()

	got := filter.ApplyFilter(
		event("x", []*modelv1.AttributeEntry{attr("keep", "v"), attr("drop", "x")}),
		eventFilter(spec([]string{"x"}, []string{"keep"})),
	)
	if got == nil || len(got.GetAttributes()) != 1 || got.GetAttributes()[0].GetKey() != "keep" {
		t.Fatalf("only projected attributes may survive, got %v", keysOf(got))
	}
}

func TestApplyFilterPreservesIdentityFields(t *testing.T) {
	t.Parallel()

	source := event("approval.granted", []*modelv1.AttributeEntry{attr("approver", "alice")}, "parent-1")
	got := filter.ApplyFilter(source, eventFilter(spec([]string{"approval.granted"}, []string{"approver"})))
	if got == nil {
		t.Fatal("event must survive")
	}
	if got.GetKind() != "approval.granted" || got.GetId() != "test-event" ||
		got.GetSchemaVersion() != "sentinel.model.v1" {
		t.Fatalf("identity fields not preserved: %v", got)
	}
	if !slices.Equal(got.GetCausalPredecessorIds(), []string{"parent-1"}) {
		t.Fatalf("causal predecessors = %v", got.GetCausalPredecessorIds())
	}
}

func TestApplyFilterPreservesAcknowledgedEpochIncludingZero(t *testing.T) {
	t.Parallel()

	for _, epoch := range []uint64{0, 42} {
		source := &modelv1.ProducerEvent{
			Id:                      "evt",
			Kind:                    "x",
			SchemaVersion:           "sentinel.model.v1",
			AcknowledgedFilterEpoch: &epoch,
			Attributes:              []*modelv1.AttributeEntry{attr("k", "v")},
		}
		got := filter.ApplyFilter(source, eventFilter(spec([]string{"x"}, []string{"k"})))
		if got == nil {
			t.Fatal("event must survive")
		}
		if got.AcknowledgedFilterEpoch == nil {
			t.Fatalf("acknowledged epoch %d must stay present, not collapse to absent", epoch)
		}
		if *got.AcknowledgedFilterEpoch != epoch {
			t.Fatalf("acknowledged epoch = %d, want %d", *got.AcknowledgedFilterEpoch, epoch)
		}
	}

	absent := &modelv1.ProducerEvent{Id: "evt", Kind: "x", Attributes: []*modelv1.AttributeEntry{attr("k", "v")}}
	got := filter.ApplyFilter(absent, eventFilter(spec([]string{"x"}, []string{"k"})))
	if got == nil || got.AcknowledgedFilterEpoch != nil {
		t.Fatal("an absent acknowledged epoch must stay absent")
	}
}

func TestApplyFilterPreservesCapabilitiesSensitivityAndTiming(t *testing.T) {
	t.Parallel()

	source := &modelv1.ProducerEvent{
		Id:            "evt",
		Kind:          "x",
		SchemaVersion: "sentinel.model.v1",
		Sequence:      &modelv1.SequenceCoordinate{Epoch: 3, Sequence: 9},
		OccurrenceTime: &modelv1.OccurrenceTime{
			ClockDomainId: "unix",
			Nanoseconds:   &modelv1.Int128{High: 0, Low: 1700000000123456789},
		},
		ClaimedCapabilities: []modelv1.SourceCapability{
			modelv1.SourceCapability_SOURCE_CAPABILITY_CAUSAL_EDGES,
		},
		ClaimedSensitivity: modelv1.Sensitivity_SENSITIVITY_CONFIDENTIAL,
		Attributes:         []*modelv1.AttributeEntry{attr("k", "v")},
	}
	got := filter.ApplyFilter(source, eventFilter(spec([]string{"x"}, []string{"k"})))
	if got == nil {
		t.Fatal("event must survive")
	}
	if got.GetSequence().GetSequence() != 9 || got.GetSequence().GetEpoch() != 3 {
		t.Fatalf("sequence not preserved: %v", got.GetSequence())
	}
	if got.GetOccurrenceTime().GetClockDomainId() != "unix" ||
		got.GetOccurrenceTime().GetNanoseconds().GetLow() != 1700000000123456789 {
		t.Fatalf("occurrence time not preserved: %v", got.GetOccurrenceTime())
	}
	if !slices.Equal(got.GetClaimedCapabilities(), source.GetClaimedCapabilities()) {
		t.Fatalf("claimed capabilities not preserved: %v", got.GetClaimedCapabilities())
	}
	if got.GetClaimedSensitivity() != modelv1.Sensitivity_SENSITIVITY_CONFIDENTIAL {
		t.Fatalf("claimed sensitivity = %v", got.GetClaimedSensitivity())
	}
}

// TestApplyFilterAllocatesAFreshAttributeSlice pins the aliasing contract. apply-filter.ts:44
// assigns event.attributes straight through in the keep-everything branch, so the projected
// event shares a slice with its input and appending to one can be observed by the other. The
// Go version always allocates.
func TestApplyFilterAllocatesAFreshAttributeSlice(t *testing.T) {
	t.Parallel()

	source := event("x", []*modelv1.AttributeEntry{attr("a", "1"), attr("b", "2")})
	got := filter.ApplyFilter(source, eventFilter(spec([]string{"x"}, nil)))
	if got == nil {
		t.Fatal("event must survive")
	}
	if len(got.GetAttributes()) != len(source.GetAttributes()) {
		t.Fatal("keep-everything must keep every attribute")
	}
	got.Attributes[0] = attr("swapped", "z")
	if source.GetAttributes()[0].GetKey() != "a" {
		t.Fatal("the projected event must not share its attribute slice with the input")
	}
	// The entries themselves remain shared: that is the documented shallow-aliasing contract.
	if got.GetAttributes()[1] != source.GetAttributes()[1] {
		t.Fatal("entries are shared by design; a deep copy is not paid for on the hot path")
	}
}

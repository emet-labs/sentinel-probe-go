package filter_test

import (
	"slices"
	"testing"

	"pgregory.net/rapid"

	"github.com/emet-labs/sentinel-probe-go/filter"
	modelv1 "github.com/emet-labs/sentinel-probe-go/gen/sentinel/model/v1"
)

// The ADR-0006 soundness obligation in executable form, ported from
// tests/filter/apply-filter.property.test.ts. The table test above is the primary gate; this
// is the generative backstop that catches the shapes nobody thought to tabulate.
//
// pgregory.net/rapid rather than testing/quick: quick is frozen, and decisively quick.Value
// cannot construct protobuf messages reflectively because generated types carry unexported
// protoimpl.MessageState/sizeCache/unknownFields. Generators are hand-written under rapid
// too, so the real wins are shrinking to a minimal counterexample and -rapid.seed /
// -rapid.failfile reproduction — which is what makes a failure actionable at all.

func attrKeyGen() *rapid.Generator[string] {
	return rapid.Custom(func(t *rapid.T) string {
		return "key." + rapid.StringMatching(`[a-z]{1,6}`).Draw(t, "attrKey")
	})
}

func kindGen() *rapid.Generator[string] {
	return rapid.Custom(func(t *rapid.T) string {
		return "kind." + rapid.StringMatching(`[a-z]{1,6}`).Draw(t, "kind")
	})
}

func eventGen() *rapid.Generator[*modelv1.ProducerEvent] {
	return rapid.Custom(func(t *rapid.T) *modelv1.ProducerEvent {
		entries := rapid.SliceOfN(rapid.Custom(func(t *rapid.T) *modelv1.AttributeEntry {
			return attr(attrKeyGen().Draw(t, "key"), rapid.StringN(0, 20, 20).Draw(t, "value"))
		}), 0, 5).Draw(t, "attributes")
		return &modelv1.ProducerEvent{
			Id:                   "prop-event",
			Kind:                 kindGen().Draw(t, "eventKind"),
			SchemaVersion:        "sentinel.model.v1",
			Attributes:           entries,
			CausalPredecessorIds: rapid.SliceOfN(rapid.StringN(0, 12, 12), 0, 3).Draw(t, "causal"),
		}
	})
}

func filterGen() *rapid.Generator[*modelv1.EventFilter] {
	return rapid.Custom(func(t *rapid.T) *modelv1.EventFilter {
		specs := rapid.SliceOfN(rapid.Custom(func(t *rapid.T) *modelv1.SpecificationFilter {
			return spec(
				rapid.SliceOfN(kindGen(), 0, 3).Draw(t, "specKinds"),
				rapid.SliceOfN(attrKeyGen(), 0, 5).Draw(t, "projectedKeys"),
			)
		}), 0, 5).Draw(t, "specifications")
		epoch := uint64(1)
		return &modelv1.EventFilter{Epoch: &epoch, Specifications: specs}
	})
}

func selects(eventKind string, specKinds []string) bool {
	return len(specKinds) == 0 || slices.Contains(specKinds, eventKind)
}

func findEntry(entries []*modelv1.AttributeEntry, key string) *modelv1.AttributeEntry {
	for _, entry := range entries {
		if entry.GetKey() == key {
			return entry
		}
	}
	return nil
}

// TestApplyFilterSoundness: for every event e and filter f, and every spec s in f that
// selects e, the projection e' satisfies (a) e' is not nil, (b) e'.kind == e.kind,
// (c) e'.causal_predecessor_ids == e.causal_predecessor_ids, and (d) every attribute of e
// whose key is in s's projection set — or all of them, when that set is empty — survives with
// its original value. And if no spec selects, e' is nil.
func TestApplyFilterSoundness(t *testing.T) {
	t.Parallel()

	rapid.Check(t, func(t *rapid.T) {
		event := eventGen().Draw(t, "event")
		eventFilter := filterGen().Draw(t, "filter")

		result := filter.ApplyFilter(event, eventFilter)

		anySelects := false
		for _, spec := range eventFilter.GetSpecifications() {
			match := spec.GetEventMatch()
			if !selects(event.GetKind(), match.GetEventKinds()) {
				continue
			}
			anySelects = true

			if result == nil {
				t.Fatalf("a selecting spec %v must not drop event kind %q", match.GetEventKinds(), event.GetKind())
			}
			if result.GetKind() != event.GetKind() {
				t.Fatalf("kind = %q, want %q", result.GetKind(), event.GetKind())
			}
			if !slices.Equal(result.GetCausalPredecessorIds(), event.GetCausalPredecessorIds()) {
				t.Fatalf("causal predecessors = %v, want %v",
					result.GetCausalPredecessorIds(), event.GetCausalPredecessorIds())
			}

			if len(match.GetProjectedAttributeKeys()) == 0 {
				if len(result.GetAttributes()) != len(event.GetAttributes()) {
					t.Fatalf("an empty projection set must keep all %d attributes, kept %d",
						len(event.GetAttributes()), len(result.GetAttributes()))
				}
				for _, original := range event.GetAttributes() {
					if findEntry(result.GetAttributes(), original.GetKey()) == nil {
						t.Fatalf("attribute %q dropped despite a project-all spec", original.GetKey())
					}
				}
				continue
			}
			for _, key := range match.GetProjectedAttributeKeys() {
				original := findEntry(event.GetAttributes(), key)
				if original == nil {
					continue
				}
				kept := findEntry(result.GetAttributes(), key)
				if kept == nil {
					t.Fatalf("projected attribute %q dropped", key)
				}
				if kept.GetValue().GetStringValue() != original.GetValue().GetStringValue() {
					t.Fatalf("attribute %q value changed: %q -> %q", key,
						original.GetValue().GetStringValue(), kept.GetValue().GetStringValue())
				}
			}
		}

		if !anySelects && result != nil {
			t.Fatalf("no spec selects kind %q, so the event must be dropped", event.GetKind())
		}
	})
}

// TestApplyFilterAlwaysPreservesCausalPredecessors states the causal-skeleton half of the
// obligation on its own: whenever the event survives at all, its causal edges are intact.
func TestApplyFilterAlwaysPreservesCausalPredecessors(t *testing.T) {
	t.Parallel()

	rapid.Check(t, func(t *rapid.T) {
		event := eventGen().Draw(t, "event")
		eventFilter := filterGen().Draw(t, "filter")

		result := filter.ApplyFilter(event, eventFilter)
		if result == nil {
			return
		}
		if !slices.Equal(result.GetCausalPredecessorIds(), event.GetCausalPredecessorIds()) {
			t.Fatalf("causal predecessors = %v, want %v",
				result.GetCausalPredecessorIds(), event.GetCausalPredecessorIds())
		}
	})
}

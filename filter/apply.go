// Package filter implements attribute-level Event Filter projection (ADR-0006).
// Go analog of sdk/typescript/src/filter/apply-filter.ts.
//
// This is RELEVANCE PROJECTION, not sampling: it never drops a relevant event, never drops an
// attribute any selecting Specification could need, and never invents data. Where the answer
// is uncertain it over-approximates upward — keeping more than strictly necessary is sound;
// keeping less is not.
package filter

import (
	modelv1 "github.com/emet-labs/sentinel-probe-go/gen/sentinel/model/v1"
	"github.com/emet-labs/sentinel-probe-go/internal/specmatch"
)

// ApplyFilter projects event against filter. It returns a possibly attribute-trimmed
// ProducerEvent when at least one SpecificationFilter's EventMatch selects the event, and nil
// when none does, meaning the event is irrelevant to every Specification and can be dropped
// entirely.
//
// The algorithm mirrors apply-filter.ts:19-70 step for step:
//
//  1. collect the SpecificationFilters whose EventMatch selects the event;
//  2. none selecting means drop, sound because no Specification depends on the event;
//  3. if ANY selecting spec has an empty projected_attribute_keys, keep every attribute
//     (over-approximate upward: that spec might need any of them);
//  4. otherwise keep the union of the selecting specs' projected keys;
//  5. rebuild the event with every other field unchanged. causal_predecessor_ids is NEVER
//     trimmed — it is the causal skeleton, not an attribute.
//
// Aliasing contract: the returned event always carries a FRESHLY ALLOCATED attribute slice,
// including in the keep-everything branch where the TypeScript reference aliases
// event.attributes directly. The *AttributeEntry elements themselves are still shared with
// the input, so a caller must not mutate an entry in place. proto.Clone would make the copy
// deep, and is deliberately not paid for on the emit hot path.
//
// A nil filter behaves as a filter with no specifications, i.e. drop.
func ApplyFilter(event *modelv1.ProducerEvent, filter *modelv1.EventFilter) *modelv1.ProducerEvent {
	if event == nil {
		return nil
	}

	// 1. Collect the SpecificationFilters whose EventMatch selects the event.
	var selecting []*modelv1.SpecificationFilter
	for _, spec := range filter.GetSpecifications() {
		if specmatch.Selects(spec, event) {
			selecting = append(selecting, spec)
		}
	}

	// 2. No spec selects: drop entirely.
	if len(selecting) == 0 {
		return nil
	}

	// 3. Any selecting spec with an empty projection set means keep everything.
	projectAll := false
	for _, spec := range selecting {
		if len(spec.GetEventMatch().GetProjectedAttributeKeys()) == 0 {
			projectAll = true
			break
		}
	}

	// 4. Otherwise keep the union of projected keys.
	trimmed := make([]*modelv1.AttributeEntry, 0, len(event.GetAttributes()))
	if projectAll {
		trimmed = append(trimmed, event.GetAttributes()...)
	} else {
		projected := make(map[string]struct{})
		for _, spec := range selecting {
			for _, key := range spec.GetEventMatch().GetProjectedAttributeKeys() {
				projected[key] = struct{}{}
			}
		}
		for _, entry := range event.GetAttributes() {
			if _, ok := projected[entry.GetKey()]; ok {
				trimmed = append(trimmed, entry)
			}
		}
	}

	// 5. Rebuild with every other field unchanged.
	return &modelv1.ProducerEvent{
		Id:                      event.GetId(),
		Sequence:                event.GetSequence(),
		SchemaVersion:           event.GetSchemaVersion(),
		AcknowledgedFilterEpoch: event.AcknowledgedFilterEpoch,
		Kind:                    event.GetKind(),
		OccurrenceTime:          event.GetOccurrenceTime(),
		Attributes:              trimmed,
		ClaimedCapabilities:     event.GetClaimedCapabilities(),
		ClaimedSensitivity:      event.GetClaimedSensitivity(),
		CausalPredecessorIds:    event.GetCausalPredecessorIds(), // never trimmed
	}
}

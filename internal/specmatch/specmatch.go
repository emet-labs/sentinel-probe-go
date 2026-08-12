// Package specmatch holds the single definition of "does this SpecificationFilter select this
// ProducerEvent", shared by the filter and enforcement packages.
//
// It lives under internal/ deliberately. In the TypeScript reference, specSelects is exported
// from apply-filter.ts so the enforcement gate can reuse it — that reuse was a review fix, to
// stop the gate carrying a second, drifting copy of the predicate — but it is NOT re-exported
// from src/index.ts, so it is not public SDK API. Go has no module-internal export, so putting
// the predicate in a normal package would widen the public surface relative to the reference.
// internal/ reproduces the reference's visibility exactly at the cost of one directory.
package specmatch

import (
	"slices"

	modelv1 "github.com/emet-labs/sentinel/sdk/go/gen/sentinel/model/v1"
)

// Selects reports whether spec's EventMatch selects event: its event_kinds is empty, or it
// includes event.kind. A spec with no EventMatch at all selects everything, defensively — the
// same over-approximate-upward choice ADR-0006 requires of the projection itself.
//
// The generated nil-safe getters give this the optional-chaining semantics of
// apply-filter.ts:77-81 for free: GetEventMatch on a nil spec is nil, GetEventKinds on a nil
// EventMatch is nil, and len(nil) == 0.
func Selects(spec *modelv1.SpecificationFilter, event *modelv1.ProducerEvent) bool {
	kinds := spec.GetEventMatch().GetEventKinds()
	if len(kinds) == 0 {
		return true
	}
	return slices.Contains(kinds, event.GetKind())
}

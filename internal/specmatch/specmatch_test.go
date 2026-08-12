package specmatch_test

import (
	"testing"

	modelv1 "github.com/emet-labs/sentinel/sdk/go/gen/sentinel/model/v1"
	"github.com/emet-labs/sentinel/sdk/go/internal/specmatch"
)

func TestSelects(t *testing.T) {
	t.Parallel()

	withKinds := func(kinds ...string) *modelv1.SpecificationFilter {
		return &modelv1.SpecificationFilter{EventMatch: &modelv1.EventMatch{EventKinds: kinds}}
	}

	cases := []struct {
		name  string
		spec  *modelv1.SpecificationFilter
		event *modelv1.ProducerEvent
		want  bool
	}{
		{"empty event_kinds matches every kind", withKinds(), &modelv1.ProducerEvent{Kind: "anything"}, true},
		{"membership hit", withKinds("a", "b"), &modelv1.ProducerEvent{Kind: "b"}, true},
		{"membership miss", withKinds("a", "b"), &modelv1.ProducerEvent{Kind: "c"}, false},
		{"kinds are exact, not prefixes", withKinds("order"), &modelv1.ProducerEvent{Kind: "order.charged"}, false},
		{"no EventMatch selects defensively", &modelv1.SpecificationFilter{}, &modelv1.ProducerEvent{Kind: "x"}, true},
		{"nil spec selects defensively", nil, &modelv1.ProducerEvent{Kind: "x"}, true},
		{"empty kind can still be matched", withKinds(""), &modelv1.ProducerEvent{}, true},
		{"empty kind against a non-empty set", withKinds("a"), &modelv1.ProducerEvent{}, false},
	}
	for _, tc := range cases {
		if got := specmatch.Selects(tc.spec, tc.event); got != tc.want {
			t.Errorf("%s: Selects = %v, want %v", tc.name, got, tc.want)
		}
	}
}

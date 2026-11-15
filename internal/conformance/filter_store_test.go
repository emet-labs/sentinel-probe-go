package conformance

import (
	"bytes"
	"encoding/json"
	"fmt"
	"testing"

	"github.com/emet-labs/sentinel-probe-go/client"
	modelv1 "github.com/emet-labs/sentinel-probe-go/gen/sentinel/model/v1"
)

// The filter_store suite (manifest-v1.json, 1.4.0) runs through the production FilterStore.
// Epochs are decimal strings parsed as uint64, so the full unsigned range is exact.

type filterStoreFilter struct {
	Label string  `json:"label"`
	Epoch *string `json:"epoch"`
}

type filterStoreStep struct {
	Op     string             `json:"op"`
	Filter *filterStoreFilter `json:"filter"`
	Epoch  *string            `json:"epoch"`
	// Expected is {applied?, held} for set and a boolean for should_refresh.
	Expected rawExpected `json:"expected"`
}

type rawExpected struct {
	Applied *bool
	Held    *filterStoreFilter
	Refresh *bool
}

func decodeStrictBytes(data []byte, out any) error {
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	return decoder.Decode(out)
}

func (e *rawExpected) UnmarshalJSON(data []byte) error {
	switch string(data) {
	case "true", "false":
		value := string(data) == "true"
		e.Refresh = &value
		return nil
	}
	type plain struct {
		Applied *bool              `json:"applied"`
		Held    *filterStoreFilter `json:"held"`
	}
	var decoded plain
	if err := decodeStrictBytes(data, &decoded); err != nil {
		return err
	}
	e.Applied, e.Held = decoded.Applied, decoded.Held
	return nil
}

func TestFilterStoreVectors(t *testing.T) {
	var suite struct {
		FormatVersion string `json:"format_version"`
		Kind          string `json:"kind"`
		Cases         []struct {
			ID      string             `json:"id"`
			Initial *filterStoreFilter `json:"initial"`
			Steps   []filterStoreStep  `json:"steps"`
		} `json:"cases"`
	}
	decodeStrict(t, manifestSuitePath(t, "filter_store"), &suite)
	if suite.FormatVersion != "1.2.0" || suite.Kind != "filter_store" || len(suite.Cases) == 0 {
		t.Fatal("unsupported filter_store suite")
	}
	for _, vector := range suite.Cases {
		vector := vector
		t.Run(vector.ID, func(t *testing.T) {
			var applied *modelv1.EventFilter
			if vector.Initial != nil {
				applied = storeFilter(t, vector.Initial)
			}
			store := client.NewFilterStore(applied)
			// The held pointer is asserted until an equal-epoch set, which pins only content.
			trackPointer := true
			for index, step := range vector.Steps {
				at := fmt.Sprintf("step %d", index)
				switch step.Op {
				case "should_refresh":
					if step.Expected.Refresh == nil {
						t.Fatalf("%s: should_refresh needs a boolean", at)
					}
					if got := store.ShouldRefresh(storeEpoch(t, step.Epoch)); got != *step.Expected.Refresh {
						t.Errorf("%s: should_refresh = %v, want %v", at, got, *step.Expected.Refresh)
					}
				case "set":
					offered := storeFilter(t, step.Filter)
					replaced := store.Set(offered)
					if step.Expected.Applied == nil {
						trackPointer = false
					} else {
						if replaced != *step.Expected.Applied {
							t.Errorf("%s: applied = %v, want %v", at, replaced, *step.Expected.Applied)
						}
						if replaced {
							applied = offered
						}
					}
					held := store.Get()
					if trackPointer && held != applied {
						t.Errorf("%s: held pointer is not the last applied Filter", at)
					}
					want := step.Expected.Held
					if held == nil || len(held.GetSpecifications()) != 1 || held.GetSpecifications()[0].GetSpecificationId() != want.Label {
						t.Errorf("%s: held label is not %s", at, want.Label)
					}
					assertEpoch(t, at, held.Epoch, storeEpoch(t, want.Epoch))
					assertEpoch(t, at, store.Epoch(), storeEpoch(t, want.Epoch))
				default:
					t.Fatalf("%s: unknown op %s", at, step.Op)
				}
			}
		})
	}
}

func storeFilter(t *testing.T, fixture *filterStoreFilter) *modelv1.EventFilter {
	t.Helper()
	return &modelv1.EventFilter{
		Epoch:          storeEpoch(t, fixture.Epoch),
		Specifications: []*modelv1.SpecificationFilter{{SpecificationId: fixture.Label}},
	}
}

func storeEpoch(t *testing.T, raw *string) *uint64 {
	t.Helper()
	if raw == nil {
		return nil
	}
	value := parseUint64(t, *raw)
	return &value
}

func assertEpoch(t *testing.T, at string, got, want *uint64) {
	t.Helper()
	if (got == nil) != (want == nil) || (got != nil && *got != *want) {
		t.Errorf("%s: epoch = %v, want %v", at, got, want)
	}
}

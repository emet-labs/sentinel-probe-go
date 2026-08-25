package conformance

import (
	"bytes"
	"encoding/json"
	"math/big"
	"os"
	"path/filepath"
	"runtime"
	"testing"

	modelv1 "github.com/emet-labs/sentinel/sdk/go/gen/sentinel/model/v1"
	int128codec "github.com/emet-labs/sentinel/sdk/go/int128"
	"github.com/emet-labs/sentinel/sdk/go/internal/specmatch"
)

const version = "1.0.0"

type envelope struct {
	FormatVersion string          `json:"format_version"`
	Kind          string          `json:"kind"`
	Cases         json.RawMessage `json:"cases"`
}

type intCase struct {
	ID    string `json:"id"`
	Value string `json:"value"`
	High  string `json:"high"`
	Low   string `json:"low"`
}

type matchCase struct {
	ID                  string `json:"id"`
	SpecificationFilter struct {
		EventMatch *struct {
			EventKinds             []string `json:"event_kinds"`
			DeliveryMode          string   `json:"delivery_mode"`
			ProjectedAttributeKeys []string `json:"projected_attribute_keys"`
		} `json:"event_match"`
		FailMode string `json:"fail_mode"`
	} `json:"specification_filter"`
	ProducerEvent struct {
		Kind string `json:"kind"`
	} `json:"producer_event"`
	Expected bool `json:"expected"`
}

func fixtureRoot(t *testing.T) string {
	t.Helper()
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("runtime.Caller failed")
	}
	return filepath.Clean(filepath.Join(filepath.Dir(file), "..", "..", "..", "..", "testdata", "probe-sdk-conformance"))
}

func decodeStrict(t *testing.T, path string, out any) {
	t.Helper()
	data, err := os.ReadFile(filepath.Join(fixtureRoot(t), path))
	if err != nil {
		t.Fatal(err)
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(out); err != nil {
		t.Fatalf("%s: %v", path, err)
	}
	if decoder.More() {
		t.Fatalf("%s: trailing JSON", path)
	}
}

func unique(t *testing.T, ids []string) {
	t.Helper()
	seen := make(map[string]struct{}, len(ids))
	for _, id := range ids {
		if id == "" {
			t.Fatal("empty case id")
		}
		if _, exists := seen[id]; exists {
			t.Fatalf("duplicate case id %q", id)
		}
		seen[id] = struct{}{}
	}
}

func TestInt128Vectors(t *testing.T) {
	var suite struct {
		FormatVersion string    `json:"format_version"`
		Kind          string    `json:"kind"`
		Cases         []intCase `json:"cases"`
	}
	decodeStrict(t, "int128-v1.json", &suite)
	if suite.FormatVersion != version || suite.Kind != "int128" {
		t.Fatal("unsupported suite")
	}
	ids := make([]string, 0, len(suite.Cases))
	for _, vector := range suite.Cases {
		ids = append(ids, vector.ID)
		value, ok := new(big.Int).SetString(vector.Value, 10)
		if !ok {
			t.Fatalf("%s: invalid value", vector.ID)
		}
		high, ok := new(big.Int).SetString(vector.High, 10)
		if !ok || !high.IsInt64() {
			t.Fatalf("%s: invalid high", vector.ID)
		}
		low, ok := new(big.Int).SetString(vector.Low, 10)
		if !ok || low.Sign() < 0 || low.BitLen() > 64 {
			t.Fatalf("%s: invalid low", vector.ID)
		}
		encoded := int128codec.FromBigInt(value)
		if encoded.High != high.Int64() || encoded.Low != low.Uint64() {
			t.Errorf("%s: encoded words = (%d,%d)", vector.ID, encoded.High, encoded.Low)
		}
		decoded := int128codec.ToBigInt(&modelv1.Int128{High: high.Int64(), Low: low.Uint64()})
		if decoded.Cmp(value) != 0 {
			t.Errorf("%s: decoded = %s, want %s", vector.ID, decoded, value)
		}
	}
	unique(t, ids)
}

func TestSpecMatchVectors(t *testing.T) {
	var suite struct {
		FormatVersion string      `json:"format_version"`
		Kind          string      `json:"kind"`
		Cases         []matchCase `json:"cases"`
	}
	decodeStrict(t, "spec-match-v1.json", &suite)
	if suite.FormatVersion != version || suite.Kind != "spec_match" {
		t.Fatal("unsupported suite")
	}
	ids := make([]string, 0, len(suite.Cases))
	for _, vector := range suite.Cases {
		ids = append(ids, vector.ID)
		spec := &modelv1.SpecificationFilter{}
		if vector.SpecificationFilter.EventMatch != nil {
			spec.EventMatch = &modelv1.EventMatch{
				EventKinds:             vector.SpecificationFilter.EventMatch.EventKinds,
				ProjectedAttributeKeys: vector.SpecificationFilter.EventMatch.ProjectedAttributeKeys,
			}
		}
		got := specmatch.Selects(spec, &modelv1.ProducerEvent{Kind: vector.ProducerEvent.Kind})
		if got != vector.Expected {
			t.Errorf("%s: Selects = %v, want %v", vector.ID, got, vector.Expected)
		}
	}
	unique(t, ids)
}

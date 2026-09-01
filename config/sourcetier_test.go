package config_test

import (
	"strings"
	"testing"

	"github.com/emet-labs/sentinel-probe-go/config"
	modelv1 "github.com/emet-labs/sentinel-probe-go/gen/sentinel/model/v1"
)

func mustLoad(t *testing.T, document string) config.SourceTierConfig {
	t.Helper()
	loaded, err := config.LoadSourceTierConfig([]byte(document))
	if err != nil {
		t.Fatalf("LoadSourceTierConfig: %v", err)
	}
	return loaded
}

func TestValidConfigParses(t *testing.T) {
	t.Parallel()

	loaded := mustLoad(t, `{
		"gateway.tool-calls": {"tier": "ANCHOR"},
		"agent.runtime": {"tier": "CONTRIBUTING"}
	}`)
	if len(loaded) != 2 {
		t.Fatalf("parsed %d entries, want 2", len(loaded))
	}
}

func TestTierForHandleResolvesBothTiers(t *testing.T) {
	t.Parallel()

	loaded := mustLoad(t, `{
		"gateway.tool-calls": {"tier": "ANCHOR"},
		"agent.runtime": {"tier": "CONTRIBUTING"}
	}`)

	anchor, err := config.TierForHandle(loaded, "gateway.tool-calls")
	if err != nil || anchor != modelv1.SourceTier_SOURCE_TIER_ANCHOR {
		t.Fatalf("anchor = %v, err = %v", anchor, err)
	}
	contributing, err := config.TierForHandle(loaded, "agent.runtime")
	if err != nil || contributing != modelv1.SourceTier_SOURCE_TIER_CONTRIBUTING {
		t.Fatalf("contributing = %v, err = %v", contributing, err)
	}
}

// TestUndeclaredHandleIsAnError: never a silent default. A source whose tier nobody declared
// has no business claiming one.
func TestUndeclaredHandleIsAnError(t *testing.T) {
	t.Parallel()

	loaded := mustLoad(t, `{"gateway.tool-calls": {"tier": "ANCHOR"}}`)
	tier, err := config.TierForHandle(loaded, "unknown.handle")
	if err == nil {
		t.Fatal("an undeclared handle must be an error")
	}
	if tier != modelv1.SourceTier_SOURCE_TIER_UNSPECIFIED {
		t.Fatalf("tier = %v, want the unspecified zero value alongside the error", tier)
	}
	if !strings.Contains(err.Error(), `no entry for source_handle "unknown.handle"`) {
		t.Fatalf("error = %q, want it to name the handle", err)
	}
}

func TestEmptyConfigNeverDefaults(t *testing.T) {
	t.Parallel()

	loaded := mustLoad(t, `{}`)
	if len(loaded) != 0 {
		t.Fatalf("parsed %d entries, want 0", len(loaded))
	}
	if _, err := config.TierForHandle(loaded, "any.handle"); err == nil {
		t.Fatal("an empty config must not resolve any handle")
	}
}

func TestInvalidTierValueIsRejected(t *testing.T) {
	t.Parallel()

	for _, document := range []string{
		`{"bad.source": {"tier": "PRIMARY"}}`,
		`{"bad.source": {"tier": "anchor"}}`, // case-sensitive
		`{"bad.source": {"tier": ""}}`,
		`{"bad.source": {"tier": "SOURCE_TIER_ANCHOR"}}`, // the proto spelling is not the config spelling
	} {
		if _, err := config.LoadSourceTierConfig([]byte(document)); err == nil {
			t.Errorf("%s must be rejected", document)
		}
	}
}

func TestNonStringTierIsRejected(t *testing.T) {
	t.Parallel()

	for _, document := range []string{
		`{"bad.source": {"tier": 123}}`,
		`{"bad.source": {"tier": null}}`,
		`{"bad.source": {"tier": ["ANCHOR"]}}`,
	} {
		if _, err := config.LoadSourceTierConfig([]byte(document)); err == nil {
			t.Errorf("%s must be rejected", document)
		}
	}
}

func TestMissingTierIsRejected(t *testing.T) {
	t.Parallel()

	if _, err := config.LoadSourceTierConfig([]byte(`{"bad.source": {"note": "x"}}`)); err == nil {
		t.Fatal("an entry without a tier must be rejected")
	}
}

func TestNonObjectConfigIsRejected(t *testing.T) {
	t.Parallel()

	for _, document := range []string{`"not-an-object"`, `[]`, `42`, `null`, `not json at all`} {
		if _, err := config.LoadSourceTierConfig([]byte(document)); err == nil {
			t.Errorf("%s must be rejected", document)
		}
	}
}

func TestNonObjectEntryIsRejected(t *testing.T) {
	t.Parallel()

	if _, err := config.LoadSourceTierConfig([]byte(`{"bad.source": "ANCHOR"}`)); err == nil {
		t.Fatal("an entry that is not an object must be rejected")
	}
}

// TestExtraKeysArePreserved mirrors zod's .passthrough(): a deployment may carry its own
// annotations alongside the tier, and the SDK is not the right place to police them.
func TestExtraKeysArePreserved(t *testing.T) {
	t.Parallel()

	loaded := mustLoad(t, `{"gateway.tool-calls": {"tier": "ANCHOR", "extra": "metadata", "owner": {"team": "platform"}}}`)
	entry := loaded["gateway.tool-calls"]
	if entry.Tier != config.TierAnchor {
		t.Fatalf("Tier = %q", entry.Tier)
	}
	if entry.Extra["extra"] != "metadata" {
		t.Fatalf("Extra[extra] = %v, want the passthrough value", entry.Extra["extra"])
	}
	owner, ok := entry.Extra["owner"].(map[string]any)
	if !ok || owner["team"] != "platform" {
		t.Fatalf("Extra[owner] = %v, want the nested object preserved", entry.Extra["owner"])
	}
	if _, leaked := entry.Extra["tier"]; leaked {
		t.Fatal("tier must not be duplicated into Extra")
	}
}

func TestDottedHandlesAreValidKeys(t *testing.T) {
	t.Parallel()

	loaded := mustLoad(t, `{
		"gateway.tool-calls": {"tier": "ANCHOR"},
		"agent.runtime.v2": {"tier": "CONTRIBUTING"}
	}`)
	anchor, err := config.TierForHandle(loaded, "gateway.tool-calls")
	if err != nil || anchor != modelv1.SourceTier_SOURCE_TIER_ANCHOR {
		t.Fatalf("anchor = %v, err = %v", anchor, err)
	}
	contributing, err := config.TierForHandle(loaded, "agent.runtime.v2")
	if err != nil || contributing != modelv1.SourceTier_SOURCE_TIER_CONTRIBUTING {
		t.Fatalf("contributing = %v, err = %v", contributing, err)
	}
}

// TestParseSourceTierConfigIsParserAgnostic: a host that decodes YAML, TOML or anything else
// hands the decoded map straight in, which is why loadSourceTierConfig takes `unknown` in the
// reference.
func TestParseSourceTierConfigIsParserAgnostic(t *testing.T) {
	t.Parallel()

	loaded, err := config.ParseSourceTierConfig(map[string]any{
		"gateway.tool-calls": map[string]any{"tier": "ANCHOR", "weight": 3},
	})
	if err != nil {
		t.Fatalf("ParseSourceTierConfig: %v", err)
	}
	tier, err := config.TierForHandle(loaded, "gateway.tool-calls")
	if err != nil || tier != modelv1.SourceTier_SOURCE_TIER_ANCHOR {
		t.Fatalf("tier = %v, err = %v", tier, err)
	}
	if loaded["gateway.tool-calls"].Extra["weight"] != 3 {
		t.Fatalf("passthrough values must survive the decoder they came from: %v",
			loaded["gateway.tool-calls"].Extra)
	}
}

// TestTierForHandleRejectsAHandBuiltUnknownTier covers the branch a caller can still reach by
// constructing a SourceTierConfig literal instead of going through the parsers.
func TestTierForHandleRejectsAHandBuiltUnknownTier(t *testing.T) {
	t.Parallel()

	handBuilt := config.SourceTierConfig{"x": {Tier: "PRIMARY"}}
	tier, err := config.TierForHandle(handBuilt, "x")
	if err == nil {
		t.Fatal("an unknown tier must be an error even when hand-built")
	}
	if tier != modelv1.SourceTier_SOURCE_TIER_UNSPECIFIED {
		t.Fatalf("tier = %v", tier)
	}
}

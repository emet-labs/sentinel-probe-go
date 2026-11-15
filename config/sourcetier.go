// Package config reads a Probe's source tier from deployment configuration (ADR-0022).
// Go analog of sdk/typescript/src/config/, whose schema.ts and source-tier.ts are collapsed
// here into one file (divergence D4): without zod the "schema" is a struct plus a validation
// function, and two files would be ceremony.
//
// Tier is NEVER hard-coded. There is deliberately no list of anchor handles anywhere in this
// SDK: the deployment declares tiers, the Probe reads them at init and looks up its own
// source_handle. An undeclared handle is an error, never a silent default — defaulting would
// quietly demote or promote a source's evidentiary weight.
package config

import (
	"encoding/json"
	"fmt"

	modelv1 "github.com/emet-labs/sentinel-probe-go/gen/sentinel/model/v1"
)

// Tier values accepted in configuration. These are the deployment-facing spellings, not the
// proto constant names; TierForHandle maps them to modelv1.SourceTier.
const (
	TierAnchor       = "ANCHOR"
	TierContributing = "CONTRIBUTING"
)

// SourceEntry is one source's declared configuration.
type SourceEntry struct {
	// Tier is "ANCHOR" or "CONTRIBUTING". Any other value is rejected at parse time.
	Tier string
	// Extra holds every other key in the entry, preserved rather than rejected. This mirrors
	// the reference's zod .passthrough(): a deployment may carry its own annotations
	// alongside the tier, and the SDK is not the right place to police them.
	Extra map[string]any
}

// SourceTierConfig maps source_handle to its declared entry.
type SourceTierConfig map[string]SourceEntry

// LoadSourceTierConfig parses JSON configuration bytes.
func LoadSourceTierConfig(raw []byte) (SourceTierConfig, error) {
	var decoded any
	if err := json.Unmarshal(raw, &decoded); err != nil {
		return nil, fmt.Errorf("source-tier: invalid JSON: %w", err)
	}
	document, ok := decoded.(map[string]any)
	if !ok {
		return nil, fmt.Errorf("source-tier: config must be an object, got %T", decoded)
	}
	return ParseSourceTierConfig(document)
}

// ParseSourceTierConfig validates configuration that has already been decoded, so a host can
// use whatever parser it likes — the reference's loadSourceTierConfig takes `unknown` for the
// same reason. YAML decoding in particular stays out of the SDK: the host decodes and calls
// this.
func ParseSourceTierConfig(raw map[string]any) (SourceTierConfig, error) {
	config := make(SourceTierConfig, len(raw))
	for handle, value := range raw {
		entry, ok := value.(map[string]any)
		if !ok {
			return nil, fmt.Errorf("source-tier: entry for %q must be an object, got %T", handle, value)
		}
		tierValue, present := entry["tier"]
		if !present {
			return nil, fmt.Errorf("source-tier: entry for %q has no tier", handle)
		}
		tier, ok := tierValue.(string)
		if !ok {
			return nil, fmt.Errorf("source-tier: tier for %q must be a string, got %T", handle, tierValue)
		}
		if tier != TierAnchor && tier != TierContributing {
			return nil, fmt.Errorf("source-tier: unknown tier %q for %q, want %q or %q",
				tier, handle, TierAnchor, TierContributing)
		}

		extra := make(map[string]any, len(entry)-1)
		for key, extraValue := range entry {
			if key == "tier" {
				continue
			}
			extra[key] = extraValue
		}
		config[handle] = SourceEntry{Tier: tier, Extra: extra}
	}
	return config, nil
}

// TierForHandle resolves the proto SourceTier for a source_handle.
//
// An undeclared handle is an error. It is never SOURCE_TIER_UNSPECIFIED and never a default:
// a source whose tier nobody declared has no business claiming one.
func TierForHandle(config SourceTierConfig, sourceHandle string) (modelv1.SourceTier, error) {
	entry, ok := config[sourceHandle]
	if !ok {
		return modelv1.SourceTier_SOURCE_TIER_UNSPECIFIED,
			fmt.Errorf("source-tier: no entry for source_handle %q", sourceHandle)
	}
	switch entry.Tier {
	case TierAnchor:
		return modelv1.SourceTier_SOURCE_TIER_ANCHOR, nil
	case TierContributing:
		return modelv1.SourceTier_SOURCE_TIER_CONTRIBUTING, nil
	default:
		// Unreachable via the constructors above, which reject unknown tiers, but a caller
		// can build a SourceTierConfig literal by hand.
		return modelv1.SourceTier_SOURCE_TIER_UNSPECIFIED,
			fmt.Errorf("source-tier: unknown tier %q for source_handle %q", entry.Tier, sourceHandle)
	}
}

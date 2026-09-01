package ids_test

import (
	"testing"

	"github.com/google/uuid"

	"github.com/emet-labs/sentinel-probe-go/ids"
)

func TestGeneratedIdentifiersAreDistinctUUIDs(t *testing.T) {
	t.Parallel()

	seen := make(map[string]struct{})
	for range 128 {
		for _, id := range []string{ids.GenerateRequestID(), ids.GenerateIdempotencyKey()} {
			parsed, err := uuid.Parse(id)
			if err != nil {
				t.Fatalf("%q is not a UUID: %v", id, err)
			}
			if parsed.Version() != 4 {
				t.Fatalf("%q is UUID version %d, want 4", id, parsed.Version())
			}
			if _, duplicate := seen[id]; duplicate {
				t.Fatalf("duplicate identifier %q", id)
			}
			seen[id] = struct{}{}
		}
	}
}

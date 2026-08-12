// Package ids generates the per-call identifiers a Probe stamps into decision requests.
// Go analog of sdk/typescript/src/util/id.ts, which uses node:crypto's randomUUID.
package ids

import "github.com/google/uuid"

// GenerateRequestID returns a fresh request ID (UUID v4).
func GenerateRequestID() string {
	return uuid.NewString()
}

// GenerateIdempotencyKey returns a fresh idempotency key (UUID v4).
//
// A distinct function from GenerateRequestID even though the implementation is identical:
// the two identify different things. A retry of the same logical decision reuses the
// idempotency key while taking a new request ID, so collapsing them would make retries
// indistinguishable from fresh asks at the receiver.
func GenerateIdempotencyKey() string {
	return uuid.NewString()
}

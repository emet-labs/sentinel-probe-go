// Package enforcement implements the ADR-0023 bounded enforcement fragment for a Probe:
// ask a Sentinel decision endpoint about an ASK_AND_BLOCK event and honour the answer.
// Go analog of sdk/typescript/src/enforcement/ (enforcement-gate.ts, monotonic-budget.ts).
//
// # Which ADR-0023 gates run here
//
// ADR-0023 has five gates. Gate 2, ask-and-block delivery, is the ONLY one this package
// enforces at runtime. Gates 1 (safety fragment), 3 (hot-path-only), 4 (anchor tier) and 5
// (shadow and adversarial validation) are attested at the promotion boundary and carried in
// filter.proto's EnforcementGateEvidence. The Probe re-checks none of them: do not expect
// runtime safety_property or anchor_tier checks here, and do not add them without changing
// where authority lives.
//
// # Budget and clocks
//
// DecideRequest.remaining_transport_budget_nanoseconds is a LOCAL, MONOTONIC, RELATIVE
// budget that each hop decrements — never an absolute deadline from another clock domain.
// Gate therefore takes deadlineNs, a monotonic absolute computed by the caller as
// now + latency_budget_ns at gate entry, and reads the clock only through
// Deps.NowMonotonicNs. The SDK never picks a clock.
//
// Gate takes a context.Context because Go convention requires it and because Deps.Decide
// needs one, but it deliberately does NOT treat ctx's deadline as a budget source: conflating
// a wall-clock-ish context deadline with the proto's monotonic relative budget is a
// clock-domain error. A host that wants both sets both.
//
// The corollary, which is easy to miss: Deps.Decide is normally a Connect client over
// net/http, which DOES honour a context deadline the host set. So Gate can receive
// context.DeadlineExceeded or context.Canceled back from Decide. Those are classified as
// transport errors and routed into the aggregate fail mode, exactly like any other error,
// with Reason distinguishing a context error from a connect.CodeOf(err). Never a permit,
// never a panic. A context that is ALREADY dead when Gate is called takes the same path
// without asking at all (divergence D17) — a real Connect client would fail identically, and
// checking makes the outcome independent of whether a given Decide implementation happens to
// honour ctx.
//
// # Projection is the caller's job
//
// Gate does not call filter.ApplyFilter. The caller projects the event and passes the
// projected instance, matching the reference exactly.
package enforcement

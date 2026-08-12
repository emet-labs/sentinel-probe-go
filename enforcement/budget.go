package enforcement

// RemainingTransportBudgetNs computes the remaining transport budget from a monotonic
// absolute deadline. Analog of monotonic-budget.ts.
//
// deadlineNs is a monotonic ABSOLUTE, computed by the caller at gate entry as
// nowMonotonicNs + latency_budget_ns. nowNs is the current monotonic reading, injected.
// A passed deadline yields 0, never a negative budget.
//
// Pure: no clock access, no side effects. The clock is injected through Deps.NowMonotonicNs
// and never read inside, so budget behaviour is testable without sleeping.
func RemainingTransportBudgetNs(deadlineNs, nowNs int64) uint64 {
	remaining := deadlineNs - nowNs
	if remaining <= 0 {
		return 0
	}
	return uint64(remaining)
}

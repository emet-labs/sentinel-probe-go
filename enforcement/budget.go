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
	remaining, valid := monotonicDelta(nowNs, deadlineNs)
	if !valid {
		return 0
	}
	return remaining
}

type budgetState struct {
	anchor int64
	budget uint64
}

func monotonicDelta(from, to int64) (uint64, bool) {
	delta := uint64(to) - uint64(from)
	return delta, delta < 1<<63
}

func (s budgetState) remaining(now int64) uint64 {
	elapsed, valid := monotonicDelta(s.anchor, now)
	if !valid || elapsed >= s.budget {
		return 0
	}
	return s.budget - elapsed
}

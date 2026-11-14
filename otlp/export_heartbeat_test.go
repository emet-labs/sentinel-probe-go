package otlp

// QueuedReportWaiters is how many callers wait on the queued follow-up
// report, so tests can sequence a coalesced report without sleeping.
func QueuedReportWaiters(h *CompletenessHeartbeat) int {
	h.coalesce.Lock()
	defer h.coalesce.Unlock()
	if h.queued == nil {
		return 0
	}
	return h.queued.waiters
}

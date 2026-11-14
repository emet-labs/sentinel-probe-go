package emission

import (
	"context"
	"errors"
	"fmt"
	"log"
	"math/rand/v2"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	sdktrace "go.opentelemetry.io/otel/sdk/trace"

	"github.com/emet-labs/sentinel/sdk/go/otlp"
)

// Export error codes reported through Options.OnError (ExportError.Code).
const (
	// ExportQueueFull: the bounded queue was full, so the Event was dropped and its slot is FAILED.
	ExportQueueFull = "queue-full"
	// ExportClosed: the span ended after Shutdown. Its slot is FAILED.
	ExportClosed = "closed"
	// ExportEncode: the span broke the producer contract (a missing reserved attribute, a local parent without
	// an Event id, ...) and was never posted. Its slot is FAILED; the same span would be refused again.
	ExportEncode = "encode"
	// ExportRefused: Sentinel answered with a non-retryable HTTP status (unknown source handle, 400).
	ExportRefused = "refused"
	// ExportRejected: Sentinel answered 2xx but rejected the span (partial_success.rejected_spans).
	ExportRejected = "rejected"
	// ExportDuplicateEventID: Sentinel already holds an Event under this id and refused the redelivery as a
	// conflict. The slot stays FAILED; see the Duplicate Event id policy on AsyncOptions.
	ExportDuplicateEventID = "duplicate-event-id"
	// ExportTransportExhausted: every attempt failed with a transport error or HTTP 408/425/429/5xx. The slot is
	// FAILED and the span is kept for a later retry (AsyncOptions.RetainFailed).
	ExportTransportExhausted = "transport-exhausted"
	// ExportRetentionOverflow: the buffer of failed spans was full, so the oldest was dropped. Its slot stays FAILED
	// for the rest of the epoch: the Event is lost and complete_through will not pass it.
	ExportRetentionOverflow = "retention-overflow"
	// ExportPanic: the exporter panicked; the panic was caught. The slot is FAILED.
	ExportPanic = "panic"
)

// ExportError says why one span's export did not complete. It never reaches the host's request handler: it goes
// to Options.OnError and the rate-limited log.
type ExportError struct {
	Code string
	// Span is the span's name.
	Span string
	// Slot is the span's Sequence slot, when it has one.
	Slot    Slot
	HasSlot bool
	// Err is the cause, for example an *otlp.HTTPStatusError carrying Sentinel's response body.
	Err error
}

func (e *ExportError) Error() string {
	where := e.Span
	if e.HasSlot {
		where = fmt.Sprintf("%s (epoch %d, slot %d)", e.Span, e.Slot.Epoch, e.Slot.Number)
	}
	if e.Err == nil {
		return fmt.Sprintf("emission: export %s: %s", e.Code, where)
	}
	return fmt.Sprintf("emission: export %s: %s: %v", e.Code, where, e.Err)
}

func (e *ExportError) Unwrap() error { return e.Err }

// AsyncOptions tunes the default span processor, which exports ended spans from a bounded queue on its own
// goroutine so span.End() never waits on Sentinel. The zero value is right for a deployed service.
//
// Overflow policy: the queue holds QueueSize ended spans. When it is full, a span that ends is dropped, not
// blocked on, and its Sequence slot is marked FAILED, so complete_through stops before it and Sentinel sees an
// honest gap (never a silent pass). The drop is reported as ExportQueueFull.
//
// Retry policy: a batch that fails with a transport error or HTTP 408/425/429/5xx is retried up to MaxAttempts
// times with exponential backoff (InitialBackoff doubling to MaxBackoff, with jitter). Nothing else is retried:
// a contract or encode failure and a Sentinel rejection get the same answer every time. After the last attempt
// the slots are FAILED and the spans move to a buffer of up to RetainFailed spans that is retried every
// RetryFailedInterval and once at Shutdown, so complete_through catches up when Sentinel returns. A slot marked
// FAILED can still become exported later. A redelivery is idempotent at Sentinel.
//
// Duplicate Event id policy: Sentinel refuses a second delivery of an Event id it already holds with a
// different content or slot (partial_success names evidence_conflict or identity_conflict). The refusal is
// deterministic, so it is not retried, and the slot stays FAILED: that slot number never reached Sentinel as
// an Event, so declaring complete_through past it would overstate completeness. Missing evidence must never
// look complete. The refusal is reported as ExportDuplicateEventID. The Event under that id stays whatever
// Sentinel accepted first. Every other deterministic rejection (slot_conflict, admission, a closed boundary)
// also leaves the slot FAILED and is not retried.
type AsyncOptions struct {
	// Synchronous exports on the goroutine that ends the span, with one attempt and no retry buffer. It exists for
	// tests and one-shot tools: a slow Sentinel then slows span.End(). Everything else about the span-end path
	// (never panicking, never returning an error) is unchanged.
	Synchronous bool
	// QueueSize bounds the spans waiting to export. Zero means 2048.
	QueueSize int
	// BatchSize is the most spans one request carries. Zero means 64.
	BatchSize int
	// MaxAttempts is the attempts per batch, including the first. Zero means 5.
	MaxAttempts int
	// InitialBackoff is the wait before the second attempt. Zero means 200 ms.
	InitialBackoff time.Duration
	// MaxBackoff caps the wait between attempts. Zero means 5 s.
	MaxBackoff time.Duration
	// RetainFailed bounds the spans kept for later retry after MaxAttempts. Zero means 1024. Negative keeps none.
	RetainFailed int
	// RetryFailedInterval is how often retained spans are retried. Zero means 30 s.
	RetryFailedInterval time.Duration
	// Sleep waits d or until ctx ends, returning ctx's error then. Defaults to a timer. Injected by tests.
	Sleep func(ctx context.Context, d time.Duration) error
}

func (o AsyncOptions) withDefaults() AsyncOptions {
	if o.QueueSize <= 0 {
		o.QueueSize = 2048
	}
	if o.BatchSize <= 0 {
		o.BatchSize = 64
	}
	if o.MaxAttempts <= 0 {
		o.MaxAttempts = 5
	}
	if o.InitialBackoff <= 0 {
		o.InitialBackoff = 200 * time.Millisecond
	}
	if o.MaxBackoff <= 0 {
		o.MaxBackoff = 5 * time.Second
	}
	if o.RetainFailed == 0 {
		o.RetainFailed = 1024
	}
	if o.RetryFailedInterval <= 0 {
		o.RetryFailedInterval = 30 * time.Second
	}
	if o.Sleep == nil {
		o.Sleep = sleepTimer
	}
	return o
}

func sleepTimer(ctx context.Context, d time.Duration) error {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-timer.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// AsyncStats is a snapshot of the exporter's counters.
type AsyncStats struct {
	Queued, Retained                      int
	Exported, Dropped, Failed, Duplicates uint64
}

// AsyncProcessor is the span processor that exports ended spans to Sentinel from a bounded queue and marks each
// span's Sequence slot from Sentinel's answer. Its OnEnd never panics and never returns an error to the host.
// Build one with NewProbeTracer (the default when Options.Endpoint is set).
type AsyncProcessor struct {
	exporter  *otlp.TraceExporter
	allocator *SequenceAllocator
	options   AsyncOptions
	report    func(*ExportError)

	mu     sync.RWMutex // guards closed and sends on queue
	closed bool
	queue  chan sdktrace.ReadOnlySpan
	flush  chan chan struct{}

	ctx    context.Context
	cancel context.CancelFunc
	done   chan struct{}

	retained []sdktrace.ReadOnlySpan // owned by the worker goroutine

	exported, dropped, failed, duplicates atomic.Uint64
	retainedCount                         atomic.Int64
}

var _ sdktrace.SpanProcessor = (*AsyncProcessor)(nil)

func newAsyncProcessor(exporter *otlp.TraceExporter, allocator *SequenceAllocator, options AsyncOptions, onError func(error), logger *rateLimitedLog) *AsyncProcessor {
	options = options.withDefaults()
	p := &AsyncProcessor{exporter: exporter, allocator: allocator, options: options, done: make(chan struct{})}
	p.report = func(e *ExportError) {
		if logger != nil {
			logger.log(e)
		}
		if onError != nil {
			func() {
				defer func() { _ = recover() }() // a host callback must not break the exporter
				onError(e)
			}()
		}
	}
	p.ctx, p.cancel = context.WithCancel(context.Background())
	if options.Synchronous {
		close(p.done)
		return p
	}
	p.queue = make(chan sdktrace.ReadOnlySpan, options.QueueSize)
	p.flush = make(chan chan struct{})
	go p.run()
	return p
}

// OnStart implements sdktrace.SpanProcessor.
func (*AsyncProcessor) OnStart(context.Context, sdktrace.ReadWriteSpan) {}

// OnEnd queues the span. It never blocks, panics or returns an error into the host: a full queue, a closed
// processor or any internal failure marks the span's slot FAILED and is reported through OnError.
func (p *AsyncProcessor) OnEnd(span sdktrace.ReadOnlySpan) {
	defer func() {
		if r := recover(); r != nil {
			p.safeFail(span, ExportPanic, fmt.Errorf("panic: %v", r))
		}
	}()
	if p.options.Synchronous {
		p.deliver(p.ctx, []sdktrace.ReadOnlySpan{span}, 1, false)
		return
	}
	p.mu.RLock()
	defer p.mu.RUnlock()
	if p.closed {
		p.fail(span, ExportClosed, errors.New("the span ended after Shutdown"))
		return
	}
	select {
	case p.queue <- span:
	default:
		p.dropped.Add(1)
		p.fail(span, ExportQueueFull, fmt.Errorf("export queue of %d is full", p.options.QueueSize))
	}
}

// ForceFlush exports every span queued when it is called, then returns. It does not retry retained spans.
func (p *AsyncProcessor) ForceFlush(ctx context.Context) error {
	if p.options.Synchronous {
		return nil
	}
	reply := make(chan struct{})
	select {
	case p.flush <- reply:
	case <-p.done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
	select {
	case <-reply:
		return nil
	case <-p.done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Shutdown stops accepting spans, exports what is queued, makes one last attempt on retained spans, and closes
// the exporter. Call it before the final completeness heartbeat, so the heartbeat declares what was exported.
// If ctx ends first, in-flight requests are cancelled and the slots of what is left stay FAILED or in flight,
// so complete_through never covers them; the error is ctx's.
func (p *AsyncProcessor) Shutdown(ctx context.Context) error {
	p.mu.Lock()
	if !p.closed {
		p.closed = true
		if p.queue != nil {
			close(p.queue)
		}
	}
	p.mu.Unlock()
	var err error
	select {
	case <-p.done:
	case <-ctx.Done():
		p.cancel()
		err = ctx.Err()
	}
	p.cancel()
	return errors.Join(err, p.exporter.Shutdown(ctx))
}

// Stats is a snapshot of the counters.
func (p *AsyncProcessor) Stats() AsyncStats {
	queued := 0
	if p.queue != nil {
		queued = len(p.queue)
	}
	return AsyncStats{
		Queued: queued, Retained: int(p.retainedCount.Load()), Exported: p.exported.Load(),
		Dropped: p.dropped.Load(), Failed: p.failed.Load(), Duplicates: p.duplicates.Load(),
	}
}

func (p *AsyncProcessor) run() {
	defer close(p.done)
	var tick <-chan time.Time
	if p.options.RetainFailed > 0 {
		ticker := time.NewTicker(p.options.RetryFailedInterval)
		defer ticker.Stop()
		tick = ticker.C
	}
	for {
		select {
		case span, ok := <-p.queue:
			if !ok {
				p.safely(func() { p.retryRetained() })
				return
			}
			p.safely(func() { p.deliverBatch(p.collect(span)) })
		case reply := <-p.flush:
			p.safely(p.drain)
			close(reply)
		case <-tick:
			p.safely(func() { p.retryRetained() })
		}
	}
}

// safely runs f and absorbs a panic so the worker goroutine never dies and never crashes the host.
func (p *AsyncProcessor) safely(f func()) {
	defer func() {
		if r := recover(); r != nil {
			p.report(&ExportError{Code: ExportPanic, Span: "(worker)", Err: fmt.Errorf("panic: %v", r)})
		}
	}()
	f()
}

// collect takes first and up to BatchSize-1 more spans that are already queued.
func (p *AsyncProcessor) collect(first sdktrace.ReadOnlySpan) []sdktrace.ReadOnlySpan {
	batch := append(make([]sdktrace.ReadOnlySpan, 0, p.options.BatchSize), first)
	for len(batch) < p.options.BatchSize {
		select {
		case span, ok := <-p.queue:
			if !ok {
				return batch
			}
			batch = append(batch, span)
		default:
			return batch
		}
	}
	return batch
}

func (p *AsyncProcessor) drain() {
	for {
		select {
		case span, ok := <-p.queue:
			if !ok {
				return
			}
			p.deliverBatch(p.collect(span))
		default:
			return
		}
	}
}

func (p *AsyncProcessor) deliverBatch(batch []sdktrace.ReadOnlySpan) {
	p.deliver(p.ctx, batch, p.options.MaxAttempts, false)
}

// deliver posts spans, retrying a retryable failure up to attempts times. fromRetained marks a retry of spans
// whose slots are already FAILED: it marks and reports nothing new for a failure it is not the first to see.
func (p *AsyncProcessor) deliver(ctx context.Context, spans []sdktrace.ReadOnlySpan, attempts int, fromRetained bool) {
	if len(spans) == 0 {
		return
	}
	defer func() {
		if r := recover(); r != nil {
			for _, span := range spans {
				p.safeFail(span, ExportPanic, fmt.Errorf("panic: %v", r))
			}
		}
	}()
	var (
		result otlp.ExportResult
		err    error
	)
	for attempt := 1; ; attempt++ {
		result, err = p.exporter.ExportSpansResult(ctx, spans)
		if err == nil || !otlp.Retryable(err) {
			break
		}
		if attempt >= attempts {
			p.exhausted(spans, err, fromRetained)
			return
		}
		if sleepErr := p.options.Sleep(ctx, p.backoff(attempt)); sleepErr != nil {
			p.exhausted(spans, fmt.Errorf("%w (gave up waiting to retry: %v)", err, sleepErr), fromRetained)
			return
		}
	}
	switch {
	case err != nil:
		if len(spans) > 1 {
			// One bad span must not fail its neighbours: send them one by one to isolate it.
			for _, span := range spans {
				p.deliver(ctx, []sdktrace.ReadOnlySpan{span}, attempts, fromRetained)
			}
			return
		}
		code := ExportRefused
		var status *otlp.HTTPStatusError
		var refusal *otlp.Refusal
		switch {
		case errors.As(err, &status):
		case errors.As(err, &refusal):
			code = ExportEncode
		default:
			// A 2xx answer Sentinel's verdict on cannot be read (an undecodable body): the slot stays FAILED.
			code = ExportRejected
		}
		p.failOnce(spans[0], code, err, fromRetained)
	case result.Rejected > 0:
		if len(spans) > 1 {
			for _, span := range spans {
				p.deliver(ctx, []sdktrace.ReadOnlySpan{span}, attempts, fromRetained)
			}
			return
		}
		cause := otlp.DescribeRejection(result.Rejected, 1, result.Message)
		if isDuplicateEventID(result.Message) {
			p.duplicates.Add(1)
			p.failOnce(spans[0], ExportDuplicateEventID, cause, fromRetained)
			return
		}
		p.failOnce(spans[0], ExportRejected, cause, fromRetained)
	default:
		for _, span := range spans {
			p.markExported(span)
		}
		p.exported.Add(uint64(len(spans)))
	}
}

// isDuplicateEventID recognises Sentinel's refusal of an Event id it already holds.
func isDuplicateEventID(message string) bool {
	return strings.Contains(message, "evidence_conflict") || strings.Contains(message, "identity_conflict")
}

func (p *AsyncProcessor) backoff(attempt int) time.Duration {
	d := p.options.InitialBackoff
	for i := 1; i < attempt && d < p.options.MaxBackoff; i++ {
		d *= 2
	}
	d = min(d, p.options.MaxBackoff)
	return d - d/5 + time.Duration(rand.Int64N(int64(d/5)*2+1))
}

// exhausted handles a batch whose every attempt failed retryably: FAILED now, retained for later.
func (p *AsyncProcessor) exhausted(spans []sdktrace.ReadOnlySpan, err error, fromRetained bool) {
	for _, span := range spans {
		if !fromRetained {
			p.failed.Add(1)
			if slot, ok := slotOf(span); ok && p.allocator != nil {
				p.allocator.MarkFailed(slot)
			}
			p.reportSpan(span, ExportTransportExhausted, err)
		}
		if !p.options.Synchronous {
			p.retain(span)
		}
	}
}

func (p *AsyncProcessor) retain(span sdktrace.ReadOnlySpan) {
	limit := p.options.RetainFailed
	if limit < 0 {
		return
	}
	if len(p.retained) >= limit {
		dropped := p.retained[0]
		p.retained = p.retained[1:]
		p.dropped.Add(1)
		p.reportSpan(dropped, ExportRetentionOverflow, errors.New("buffer of failed spans is full"))
	}
	p.retained = append(p.retained, span)
	p.retainedCount.Store(int64(len(p.retained)))
}

// retryRetained makes one attempt per retained span batch; what still fails stays retained.
func (p *AsyncProcessor) retryRetained() {
	for len(p.retained) > 0 && p.ctx.Err() == nil {
		n := min(p.options.BatchSize, len(p.retained))
		batch := append([]sdktrace.ReadOnlySpan(nil), p.retained[:n]...)
		p.retained = append(p.retained[:0:0], p.retained[n:]...)
		p.retainedCount.Store(int64(len(p.retained)))
		before := len(p.retained)
		p.deliver(p.ctx, batch, 1, true)
		if len(p.retained) > before {
			// Sentinel is still unreachable: stop and wait for the next interval.
			return
		}
	}
}

func (p *AsyncProcessor) markExported(span sdktrace.ReadOnlySpan) {
	if slot, ok := slotOf(span); ok && p.allocator != nil {
		p.allocator.MarkExported(slot)
	}
}

// fail marks the span's slot FAILED, counts it and reports.
func (p *AsyncProcessor) fail(span sdktrace.ReadOnlySpan, code string, err error) {
	p.failed.Add(1)
	if slot, ok := slotOf(span); ok && p.allocator != nil {
		p.allocator.MarkFailed(slot)
	}
	p.reportSpan(span, code, err)
}

// failOnce is fail, except a retained span's slot is already FAILED and was already reported.
func (p *AsyncProcessor) failOnce(span sdktrace.ReadOnlySpan, code string, err error, fromRetained bool) {
	if fromRetained {
		p.reportSpan(span, code, err)
		return
	}
	p.fail(span, code, err)
}

// safeFail is fail for a path that must not panic again.
func (p *AsyncProcessor) safeFail(span sdktrace.ReadOnlySpan, code string, err error) {
	defer func() { _ = recover() }()
	p.fail(span, code, err)
}

func (p *AsyncProcessor) reportSpan(span sdktrace.ReadOnlySpan, code string, err error) {
	e := &ExportError{Code: code, Err: err}
	if span != nil {
		e.Span = span.Name()
		e.Slot, e.HasSlot = slotOf(span)
	}
	p.report(e)
}

// rateLimitedLog writes at most one line per interval, counting what it suppressed.
type rateLimitedLog struct {
	mu         sync.Mutex
	interval   time.Duration
	logf       func(format string, args ...any)
	now        func() time.Time
	last       time.Time
	suppressed int
}

func newRateLimitedLog(interval time.Duration, logf func(string, ...any)) *rateLimitedLog {
	if logf == nil {
		logf = log.Printf
	}
	if interval <= 0 {
		interval = 10 * time.Second
	}
	return &rateLimitedLog{interval: interval, logf: logf, now: time.Now}
}

func (l *rateLimitedLog) log(e *ExportError) {
	l.mu.Lock()
	now := l.now()
	if !l.last.IsZero() && now.Sub(l.last) < l.interval {
		l.suppressed++
		l.mu.Unlock()
		return
	}
	suppressed := l.suppressed
	l.suppressed, l.last = 0, now
	l.mu.Unlock()
	if suppressed > 0 {
		l.logf("sentinel probe: %v (%d similar export errors suppressed)", e, suppressed)
		return
	}
	l.logf("sentinel probe: %v", e)
}

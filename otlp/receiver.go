package otlp

import (
	"bytes"
	"compress/gzip"
	"context"
	"errors"
	"io"
	"mime"
	"net"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	modelv1 "github.com/emet-labs/sentinel-probe-go/gen/sentinel/model/v1"
	probev1 "github.com/emet-labs/sentinel-probe-go/gen/sentinel/probe/v1"
)

const uncompressedLimit = 8 << 20

// DecisionTimer arms the expiry of one decision wait for budget. It returns the channel that
// fires on expiry and a stop function, with the shape of time.NewTimer's C and Stop. A test
// supplies its own through ListenConfig.DecisionTimer so a wait runs without a wall clock.
type DecisionTimer func(budget time.Duration) (<-chan time.Time, func() bool)

func systemDecisionTimer(budget time.Duration) (<-chan time.Time, func() bool) {
	timer := time.NewTimer(budget)
	return timer.C, timer.Stop
}

// Receiver is a POST /v1/logs endpoint. It decodes Verdict, EventFilter, and
// DecideResponse. Unspecified decision actions are not stored. It binds
// loopback unless ListenWith is given another host.
type Receiver struct {
	server        *http.Server
	url           string
	addr          net.Addr
	decisionTimer DecisionTimer

	mu             sync.Mutex
	verdicts       []*modelv1.Verdict
	filters        []*modelv1.EventFilter
	decisions      map[string]*probev1.DecideResponse
	decisionErrors map[string]error
	waiters        map[string][]chan struct{}
	closed         bool
}

// ListenConfig is where a Receiver binds, and how it times a decision wait.
type ListenConfig struct {
	// Host is the interface to bind. Empty means 127.0.0.1. A Probe in a container that Sentinel reaches over a
	// network binds "0.0.0.0" (every interface) or the container's own address; loopback inside a container is
	// not reachable from another container, so Event Filters would never arrive.
	Host string
	// Port is the TCP port. Zero picks a free one.
	Port int
	// DecisionTimer replaces the timer Await arms for a decision wait. Nil means time.NewTimer.
	DecisionTimer DecisionTimer
}

// Listen serves POST /v1/logs on a loopback address. An empty addr uses
// 127.0.0.1:0. Any other host is refused; use ListenWith to bind another.
func Listen(addr string) (*Receiver, error) {
	if addr == "" {
		addr = "127.0.0.1:0"
	}
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, err
	}
	tcp, ok := listener.Addr().(*net.TCPAddr)
	if !ok || tcp.IP == nil || !tcp.IP.IsLoopback() {
		_ = listener.Close()
		return nil, refuse("non-loopback", addr)
	}
	return serve(listener, nil), nil
}

// ListenWith serves POST /v1/logs on the configured host and port. The zero ListenConfig is
// Listen("127.0.0.1:0"). A non-loopback bind exposes an unauthenticated endpoint to whoever can reach the
// port: bind it only on a network that carries Sentinel and nothing else.
func ListenWith(config ListenConfig) (*Receiver, error) {
	host := config.Host
	if host == "" {
		host = "127.0.0.1"
	}
	if config.Port < 0 || config.Port > 65535 {
		return nil, refuse("invalid-port", strconv.Itoa(config.Port))
	}
	listener, err := net.Listen("tcp", net.JoinHostPort(host, strconv.Itoa(config.Port)))
	if err != nil {
		return nil, err
	}
	return serve(listener, config.DecisionTimer), nil
}

func serve(listener net.Listener, timer DecisionTimer) *Receiver {
	if timer == nil {
		timer = systemDecisionTimer
	}
	receiver := &Receiver{
		addr:           listener.Addr(),
		decisionTimer:  timer,
		decisions:      map[string]*probev1.DecideResponse{},
		decisionErrors: map[string]error{},
		waiters:        map[string][]chan struct{}{},
	}
	receiver.server = &http.Server{
		Handler:           receiver,
		ReadHeaderTimeout: exportTimeout,
	}
	// An unspecified bind address (0.0.0.0, ::) is not dialable; advertise loopback for local use. A host
	// that tells Sentinel where to POST uses LogsURLFor with its own reachable name.
	host, port, _ := net.SplitHostPort(listener.Addr().String())
	if ip := net.ParseIP(host); ip != nil && ip.IsUnspecified() {
		host = "127.0.0.1"
	}
	receiver.url = "http://" + net.JoinHostPort(host, port) + "/v1/logs"
	go func() { _ = receiver.server.Serve(listener) }()
	return receiver
}

// Addr is the address the Receiver is bound to.
func (r *Receiver) Addr() net.Addr { return r.addr }

// Port is the TCP port the Receiver is bound to.
func (r *Receiver) Port() int {
	if tcp, ok := r.addr.(*net.TCPAddr); ok {
		return tcp.Port
	}
	return 0
}

// LogsURLFor is the /v1/logs address Sentinel should POST to when it reaches this Probe as host, for example
// the container name on the shared network.
func (r *Receiver) LogsURLFor(host string) string {
	return "http://" + net.JoinHostPort(host, strconv.Itoa(r.Port())) + "/v1/logs"
}

// LogsURL is the http://127.0.0.1:port/v1/logs address for a Sentinel on the same machine (a wildcard bind
// advertises loopback; see LogsURLFor).
func (r *Receiver) LogsURL() string { return r.url }

// Close stops the listener and fails every pending Await.
func (r *Receiver) Close() error {
	r.mu.Lock()
	if !r.closed {
		r.closed = true
		for requestID, pending := range r.waiters {
			for _, waiter := range pending {
				close(waiter)
			}
			delete(r.waiters, requestID)
		}
	}
	r.mu.Unlock()
	if r.server == nil {
		return nil
	}
	return r.server.Close()
}

// Verdicts returns the Verdicts decoded so far, oldest first.
func (r *Receiver) Verdicts() []*modelv1.Verdict {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]*modelv1.Verdict, len(r.verdicts))
	copy(out, r.verdicts)
	return out
}

// Filters returns the Event Filters decoded so far, oldest first.
func (r *Receiver) Filters() []*modelv1.EventFilter {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]*modelv1.EventFilter, len(r.filters))
	copy(out, r.filters)
	return out
}

// Response returns the DecideResponse stored for requestID without waiting. When
// the latest DecideResponse log for requestID was refused, it returns that
// refusal; no response at all is a missing-decision error. The enforcement gate
// treats either as an unresolved ask.
func (r *Receiver) Response(requestID string) (*probev1.DecideResponse, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.lookupLocked(requestID)
}

// Await returns the DecideResponse for requestID, waiting up to budget for it to
// arrive. budget is the request's remaining transport budget (ADR-0024).
//
//   - A stored response wins at once, before any wait is armed. A refused
//     DecideResponse for requestID fails at once.
//   - A budget of zero or less arms no wait and returns missing-decision.
//   - Otherwise the waiter is registered, one timer is armed for exactly budget,
//     and Await blocks until the response arrives, a refusal for requestID
//     arrives, the timer expires (a timeout error), ctx ends, or Close runs.
//
// Every error is an unresolved ask, and the gate applies its fail mode. A
// response arriving after the wait expired does not change that ask.
func (r *Receiver) Await(
	ctx context.Context,
	requestID string,
	budget time.Duration,
) (*probev1.DecideResponse, error) {
	r.mu.Lock()
	if decision, err := r.lookupLocked(requestID); decision != nil || !isMissing(err) {
		r.mu.Unlock()
		return decision, err
	}
	if r.closed {
		r.mu.Unlock()
		return nil, errReceiverClosed
	}
	if budget <= 0 {
		r.mu.Unlock()
		return nil, refuse("missing-decision", requestID)
	}
	waiter := make(chan struct{})
	r.waiters[requestID] = append(r.waiters[requestID], waiter)
	r.mu.Unlock()

	expired, stop := r.decisionTimer(budget)
	defer stop()
	select {
	case <-waiter:
	case <-expired:
	case <-ctx.Done():
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	r.removeWaiterLocked(requestID, waiter)
	if decision, err := r.lookupLocked(requestID); decision != nil || !isMissing(err) {
		return decision, err
	}
	if r.closed {
		return nil, errReceiverClosed
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return nil, refuse("timeout", requestID)
}

// errReceiverClosed fails an Await on a closed receiver.
var errReceiverClosed = refuse("receiver-closed", "")

func (r *Receiver) lookupLocked(requestID string) (*probev1.DecideResponse, error) {
	if err, ok := r.decisionErrors[requestID]; ok {
		return nil, err
	}
	if decision, ok := r.decisions[requestID]; ok {
		return decision, nil
	}
	return nil, refuse("missing-decision", requestID)
}

func isMissing(err error) bool {
	var refusal *Refusal
	return errors.As(err, &refusal) && refusal.Reason == "missing-decision"
}

func (r *Receiver) removeWaiterLocked(requestID string, waiter chan struct{}) {
	pending := r.waiters[requestID]
	for i, candidate := range pending {
		if candidate == waiter {
			pending = append(pending[:i], pending[i+1:]...)
			break
		}
	}
	if len(pending) == 0 {
		delete(r.waiters, requestID)
	} else {
		r.waiters[requestID] = pending
	}
}

// wakeLocked releases every waiter for requestID. Each re-reads the stored
// outcome under the lock.
func (r *Receiver) wakeLocked(requestID string) {
	for _, waiter := range r.waiters[requestID] {
		close(waiter)
	}
	delete(r.waiters, requestID)
}

// ServeHTTP accepts one OTLP/HTTP log export. Identity and gzip are both
// accepted. A refused body stores nothing, except that a DecideResponse refused
// after its request id was read fails the waits for that id.
func (r *Receiver) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	if req.URL.Path != "/v1/logs" || req.Method != http.MethodPost {
		http.NotFound(w, req)
		return
	}
	mediaType, _, err := mime.ParseMediaType(req.Header.Get("Content-Type"))
	if err != nil || mediaType != protobufMediaType {
		http.Error(w, "OTLP/HTTP requires application/x-protobuf", http.StatusUnsupportedMediaType)
		return
	}
	body, err := readBody(w, req)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	inbound, err := DecodeInboundLogs(body)
	if err != nil {
		var refused *DecisionRefusal
		if errors.As(err, &refused) {
			r.rememberDecisionError(refused)
		}
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	r.store(inbound)
	w.WriteHeader(http.StatusOK)
}

func (r *Receiver) rememberDecisionError(refused *DecisionRefusal) {
	r.mu.Lock()
	defer r.mu.Unlock()
	// As in the TypeScript reference, the latest log for an id wins: a refusal
	// shadows an earlier answer, and a later valid answer clears the refusal.
	r.decisionErrors[refused.RequestID] = refused
	r.wakeLocked(refused.RequestID)
}

func (r *Receiver) store(inbound *Inbound) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.verdicts = append(r.verdicts, inbound.Verdicts...)
	r.filters = append(r.filters, inbound.Filters...)
	for _, decision := range inbound.Decisions {
		requestID := decision.GetRequestId()
		delete(r.decisionErrors, requestID)
		r.decisions[requestID] = decision
		r.wakeLocked(requestID)
	}
}

func readBody(w http.ResponseWriter, req *http.Request) ([]byte, error) {
	defer req.Body.Close()
	encoding := req.Header.Values("Content-Encoding")
	var reader io.Reader = http.MaxBytesReader(w, req.Body, uncompressedLimit)
	switch len(encoding) {
	case 0:
	case 1:
		switch strings.ToLower(strings.TrimSpace(encoding[0])) {
		case "", "identity":
		case "gzip":
			gzipReader, err := gzip.NewReader(reader)
			if err != nil {
				return nil, err
			}
			defer gzipReader.Close()
			reader = io.LimitReader(gzipReader, uncompressedLimit+1)
		default:
			return nil, errors.New("otlp: content encoding must be identity or gzip")
		}
	default:
		return nil, errors.New("otlp: content encoding must be identity or gzip")
	}
	body, err := io.ReadAll(reader)
	if err != nil {
		return nil, err
	}
	if len(body) > uncompressedLimit {
		return nil, errors.New("otlp: uncompressed body exceeds the limit")
	}
	if bytes.HasPrefix(body, []byte{0x1f, 0x8b}) && (len(encoding) == 0 || !strings.EqualFold(strings.TrimSpace(encoding[0]), "gzip")) {
		return nil, errors.New("otlp: gzip body requires content-encoding gzip")
	}
	return body, nil
}

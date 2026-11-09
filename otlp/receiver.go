package otlp

import (
	"bytes"
	"compress/gzip"
	"errors"
	"io"
	"mime"
	"net"
	"net/http"
	"strings"
	"sync"

	modelv1 "github.com/emet-labs/sentinel/sdk/go/gen/sentinel/model/v1"
	probev1 "github.com/emet-labs/sentinel/sdk/go/gen/sentinel/probe/v1"
)

const uncompressedLimit = 8 << 20

// Receiver is a localhost POST /v1/logs endpoint. It decodes Verdict,
// EventFilter, and DecideResponse. Unspecified decision actions are not stored.
type Receiver struct {
	server *http.Server
	url    string

	mu        sync.Mutex
	verdicts  []*modelv1.Verdict
	filters   []*modelv1.EventFilter
	decisions map[string]*probev1.DecideResponse
}

// Listen serves POST /v1/logs on a loopback address. An empty addr uses
// 127.0.0.1:0. Any other host is refused.
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
	receiver := &Receiver{decisions: map[string]*probev1.DecideResponse{}}
	receiver.server = &http.Server{
		Handler:           receiver,
		ReadHeaderTimeout: exportTimeout,
	}
	receiver.url = "http://" + listener.Addr().String() + "/v1/logs"
	go func() { _ = receiver.server.Serve(listener) }()
	return receiver, nil
}

// LogsURL is the http://127.0.0.1:port/v1/logs address Sentinel should POST to.
func (r *Receiver) LogsURL() string { return r.url }

// Close stops the listener.
func (r *Receiver) Close() error {
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

// Response returns the DecideResponse stored for requestID. A missing response
// is an error, which the enforcement gate treats as an unresolved ask.
func (r *Receiver) Response(requestID string) (*probev1.DecideResponse, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	decision, ok := r.decisions[requestID]
	if !ok {
		return nil, refuse("missing-decision", requestID)
	}
	return decision, nil
}

// ServeHTTP accepts one OTLP/HTTP log export. Identity and gzip are both
// accepted. A refused body stores nothing.
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
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	r.store(inbound)
	w.WriteHeader(http.StatusOK)
}

func (r *Receiver) store(inbound *Inbound) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.verdicts = append(r.verdicts, inbound.Verdicts...)
	r.filters = append(r.filters, inbound.Filters...)
	for _, decision := range inbound.Decisions {
		r.decisions[decision.GetRequestId()] = decision
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

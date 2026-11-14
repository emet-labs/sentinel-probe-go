package otlp

import (
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"
)

// responseBodyLimit bounds how much of an error response is kept on an HTTPStatusError.
const responseBodyLimit = 4 << 10

// TransportError is a request that never produced an HTTP answer: a refused or reset connection, a DNS failure,
// a timeout. Nothing says Sentinel saw the batch, so redelivery is safe (Sentinel treats an identical redelivery
// as a duplicate) and the Probe's exporter retries it.
type TransportError struct{ Err error }

func (e *TransportError) Error() string { return "otlp: post: " + e.Err.Error() }
func (e *TransportError) Unwrap() error { return e.Err }

// HTTPStatusError is a non-2xx answer. It carries Sentinel's response body, which names the reason ("unknown
// source handle", a decode error), so a host log says why and not only "403 Forbidden". It unwraps to the
// *Refusal{Reason: "export-refused"} older callers matched.
type HTTPStatusError struct {
	StatusCode int
	Status     string
	// Body is the start of the response body (at most 4 KiB), with control characters left as sent.
	Body string
	// RetryAfter is the server's Retry-After on a 429 or 503 (zero when absent, unparseable or on any other
	// status). The exporter waits at least this long before the next attempt, capped at its MaxBackoff.
	RetryAfter time.Duration
	refusal    *Refusal
}

func newHTTPStatusError(response *http.Response, body []byte) *HTTPStatusError {
	text := strings.TrimSpace(string(body))
	if len(text) > responseBodyLimit {
		text = text[:responseBodyLimit]
	}
	detail := response.Status
	if text != "" {
		detail += ": " + text
	}
	return &HTTPStatusError{
		StatusCode: response.StatusCode, Status: response.Status, Body: text,
		RetryAfter: retryAfter(response),
		refusal:    &Refusal{Reason: "export-refused", Detail: detail},
	}
}

func (e *HTTPStatusError) Error() string { return e.refusal.Error() }
func (e *HTTPStatusError) Unwrap() error { return e.refusal }

// retryAfter parses Retry-After (delta-seconds or an HTTP-date), honored on 429 and 503 only.
func retryAfter(response *http.Response) time.Duration {
	if response.StatusCode != http.StatusTooManyRequests && response.StatusCode != http.StatusServiceUnavailable {
		return 0
	}
	value := strings.TrimSpace(response.Header.Get("Retry-After"))
	if value == "" {
		return 0
	}
	if seconds, err := strconv.ParseUint(value, 10, 32); err == nil {
		return time.Duration(seconds) * time.Second
	}
	if at, err := http.ParseTime(value); err == nil {
		return max(0, time.Until(at))
	}
	return 0
}

// Retryable reports whether the same batch may succeed if sent again unchanged: a transport failure, or HTTP
// 408, 425, 429, 500, 502, 503 or 504 (the Probe SDK retry policy, sdk/README.md). Validation and encode
// refusals, every other HTTP status and a Sentinel rejection are not retryable: the same bytes get the same
// answer.
func Retryable(err error) bool {
	var transport *TransportError
	if errors.As(err, &transport) {
		return true
	}
	var status *HTTPStatusError
	if errors.As(err, &status) {
		switch status.StatusCode {
		case http.StatusRequestTimeout, http.StatusTooEarly, http.StatusTooManyRequests,
			http.StatusInternalServerError, http.StatusBadGateway, http.StatusServiceUnavailable,
			http.StatusGatewayTimeout:
			return true
		}
		return false
	}
	return false
}

// DescribeRejection is the error for a batch Sentinel answered 2xx with rejected spans.
func DescribeRejection(rejected int64, total int, message string) error {
	return fmt.Errorf("otlp: Sentinel rejected %d of %d spans: %s", rejected, total, message)
}

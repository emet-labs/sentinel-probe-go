package otlp_test

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"testing"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"

	"github.com/emet-labs/sentinel/sdk/go/emission"
	modelv1 "github.com/emet-labs/sentinel/sdk/go/gen/sentinel/model/v1"
	"github.com/emet-labs/sentinel/sdk/go/otlp"
)

func TestChargeAndLinkedShipObserveAVerdict(t *testing.T) {
	stack := startPeriphery(t)
	cover(t, stack.base)
	bootstrap(t, stack.base)

	payment := newTracer(t, stack.base, "sagashop-payment")
	order := newTracer(t, stack.base, "sagashop-order")
	exportSpan(t, payment, "sagashop.payment.charge.completed", "01f0c0ffee-charge", "", []attribute.KeyValue{
		attribute.String("sagashop.payment.status", "success"),
		attribute.String("sagashop.order.id", "order-1001"),
	})
	if len(stack.receiver.Verdicts()) != 0 {
		t.Fatal("a charge alone is not a pass")
	}
	exportSpan(t, order, "sagashop.order.ship.requested", "01f0c0ffee-ship", "01f0c0ffee-charge", []attribute.KeyValue{
		attribute.String("sagashop.order.id", "order-1001"),
	})
	verdicts := stack.receiver.Verdicts()
	if len(verdicts) == 0 {
		t.Fatal("the log receiver observed no Verdict")
	}
	var matched bool
	for _, verdict := range verdicts {
		if strings.Contains(verdict.GetId(), "sagashop.p4.ship-requires-prior-charge") {
			matched = true
		}
	}
	if !matched {
		t.Fatalf("verdicts = %v", verdicts)
	}
}

func TestShipWithoutAChargeIsNotSatisfy(t *testing.T) {
	stack := startPeriphery(t)
	cover(t, stack.base)
	bootstrap(t, stack.base)

	payment := newTracer(t, stack.base, "sagashop-payment")
	order := newTracer(t, stack.base, "sagashop-order")
	exportSpan(t, payment, "sagashop.payment.charge.completed", "01f0c0ffee-declin", "", []attribute.KeyValue{
		attribute.String("sagashop.payment.status", "declined"),
		attribute.String("sagashop.order.id", "order-2002"),
	})
	exportSpan(t, order, "sagashop.order.ship.requested", "01f0c0ffee-noship", "", []attribute.KeyValue{
		attribute.String("sagashop.order.id", "order-2002"),
	})
	verdicts := stack.receiver.Verdicts()
	if len(verdicts) == 0 {
		t.Fatal("a covered ship with no successful charge produced no Verdict")
	}
	for _, verdict := range verdicts {
		if verdict.GetOutcome() == modelv1.VerdictOutcome_VERDICT_OUTCOME_SATISFY {
			t.Fatalf("a ship with no charge must not be Satisfy: %s", verdict.GetId())
		}
	}
}

type peripheryStack struct {
	base     string
	receiver *otlp.Receiver
}

func startPeriphery(t *testing.T) peripheryStack {
	t.Helper()
	bin := os.Getenv("SENTINEL_PERIPHERY_BIN")
	if bin == "" {
		t.Skip("SENTINEL_PERIPHERY_BIN is unset; harness-probe-sdk-go builds sentinel-periphery and sets it")
	}
	receiver := listen(t)
	port := freePort(t)
	base := "http://127.0.0.1:" + port
	cmd := exec.Command(bin)
	cmd.Env = peripheryEnv(base, receiver.LogsURL())
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = cmd.Process.Kill()
		_, _ = cmd.Process.Wait()
	})
	ready := make(chan struct{})
	go func() {
		scanner := bufio.NewScanner(stdout)
		for scanner.Scan() {
			if strings.Contains(scanner.Text(), "listening on") {
				close(ready)
				_, _ = io.Copy(io.Discard, stdout)
				return
			}
		}
	}()
	select {
	case <-ready:
	case <-time.After(20 * time.Second):
		t.Fatalf("sentinel-periphery did not listen: %s", stderr.String())
	}
	waitHealthy(t, base, &stderr)
	return peripheryStack{base: base, receiver: receiver}
}

func peripheryEnv(base, logsURL string) []string {
	var env []string
	for _, entry := range os.Environ() {
		if strings.HasPrefix(entry, "SENTINEL_LISTEN=") ||
			strings.HasPrefix(entry, "SENTINEL_PROBE_LOG_URL=") ||
			strings.HasPrefix(entry, "SENTINEL_STORE=") {
			continue
		}
		env = append(env, entry)
	}
	return append(env,
		"SENTINEL_STORE=memory",
		"SENTINEL_LISTEN="+strings.TrimPrefix(base, "http://"),
		"SENTINEL_PROBE_LOG_URL="+logsURL,
	)
}

func waitHealthy(t *testing.T, base string, stderr *bytes.Buffer) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		response, err := http.Get(base + "/healthz")
		if err == nil {
			response.Body.Close()
			if response.StatusCode == http.StatusOK {
				return
			}
		}
		if time.Now().After(deadline) {
			t.Fatalf("periphery healthz failed: %v %s", err, stderr.String())
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func freePort(t *testing.T) string {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	addr, ok := listener.Addr().(*net.TCPAddr)
	if !ok {
		t.Fatal("expected a tcp address")
	}
	return strconv.Itoa(addr.Port)
}

func cover(t *testing.T, base string) {
	t.Helper()
	for _, handle := range []string{"sagashop-payment", "sagashop-order"} {
		if err := otlp.ExportCompleteness(context.Background(), base, handle, completeness(1, 1)); err != nil {
			t.Fatalf("completeness %s: %v", handle, err)
		}
	}
}

func completeness(last, filterEpoch uint64) *modelv1.ProducerCompletenessSignal {
	when := &modelv1.OccurrenceTime{
		ClockDomainId: "unix",
		Nanoseconds:   &modelv1.Int128{Low: 1_700_000_000_000_000_000},
	}
	return &modelv1.ProducerCompletenessSignal{
		SequenceEpoch:           7,
		LastEmitted:             &last,
		CompleteThrough:         &last,
		ProducerReportedAt:      when,
		AcknowledgedFilterEpoch: &filterEpoch,
		Activity:                modelv1.SourceActivityState_SOURCE_ACTIVITY_STATE_ACTIVE,
		ActivitySince:           protoCloneTime(when),
	}
}

func protoCloneTime(when *modelv1.OccurrenceTime) *modelv1.OccurrenceTime {
	return &modelv1.OccurrenceTime{
		ClockDomainId:          when.GetClockDomainId(),
		Nanoseconds:            &modelv1.Int128{High: when.GetNanoseconds().GetHigh(), Low: when.GetNanoseconds().GetLow()},
		UncertaintyNanoseconds: when.GetUncertaintyNanoseconds(),
	}
}

func bootstrap(t *testing.T, base string) {
	t.Helper()
	body, err := json.Marshal(map[string]any{
		"observed_at": map[string]any{
			"sentinel_clock_id": "sentinel",
			"nanoseconds":       int64(1_700_000_000_000_000_000),
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	response, err := http.Post(base+"/v1/filters/bootstrap-cat1", "application/json", bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	payload, _ := io.ReadAll(response.Body)
	if response.StatusCode != http.StatusOK {
		t.Fatalf("bootstrap %d: %s", response.StatusCode, payload)
	}
}

func newTracer(t *testing.T, base, handle string) *emission.ProbeTracer {
	t.Helper()
	tracer := emission.NewProbeTracer(emission.Options{
		TracerName:   handle,
		ServiceName:  handle,
		Endpoint:     base,
		SourceHandle: handle,
	})
	t.Cleanup(func() {
		_ = tracer.Provider.Shutdown(context.Background())
	})
	return tracer
}

type recordedHandler struct{ err error }

func (h *recordedHandler) Handle(err error) {
	if h.err == nil {
		h.err = err
	}
}

func exportSpan(t *testing.T, tracer *emission.ProbeTracer, name, eventID, parent string, extra []attribute.KeyValue) {
	t.Helper()
	handler := &recordedHandler{}
	previous := otel.GetErrorHandler()
	otel.SetErrorHandler(handler)
	defer otel.SetErrorHandler(previous)
	start := time.Unix(1_700_000_000, 0).UTC()
	attrs := append(contractAttrs(eventID, 1, 1, parent), extra...)
	_, span := tracer.Tracer.Start(context.Background(), name,
		trace.WithTimestamp(start),
		trace.WithAttributes(attrs...))
	span.End(trace.WithTimestamp(start.Add(5 * time.Millisecond)))
	if handler.err != nil {
		t.Fatal(handler.err)
	}
}

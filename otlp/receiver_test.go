package otlp_test

import (
	"bytes"
	"compress/gzip"
	"context"
	"io"
	"net"
	"net/http"
	"strconv"
	"strings"
	"testing"

	"github.com/emet-labs/sentinel/sdk/go/client"
	"github.com/emet-labs/sentinel/sdk/go/enforcement"
	modelv1 "github.com/emet-labs/sentinel/sdk/go/gen/sentinel/model/v1"
	probev1 "github.com/emet-labs/sentinel/sdk/go/gen/sentinel/probe/v1"
	"github.com/emet-labs/sentinel/sdk/go/otlp"
	collogspb "go.opentelemetry.io/proto/otlp/collector/logs/v1"
	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	logspb "go.opentelemetry.io/proto/otlp/logs/v1"
	"google.golang.org/protobuf/proto"
)

func TestReceiverDecodesVerdictFilterAndDecision(t *testing.T) {
	receiver := listen(t)
	postLogs(t, receiver.LogsURL(), readGolden(t, "verdict.bin"), http.StatusOK)
	postLogs(t, receiver.LogsURL(), readGolden(t, "event-filter.bin"), http.StatusOK)
	postLogs(t, receiver.LogsURL(), readGolden(t, "decide-response.bin"), http.StatusOK)
	if len(receiver.Verdicts()) != 1 || len(receiver.Filters()) != 1 {
		t.Fatalf("verdicts=%d filters=%d", len(receiver.Verdicts()), len(receiver.Filters()))
	}
	decision := decodeInbound(t, "decide-response.bin").Decisions[0]
	got, err := receiver.Response(decision.GetRequestId())
	if err != nil {
		t.Fatal(err)
	}
	if got.GetAction() != decision.GetAction() {
		t.Fatalf("action = %v", got.GetAction())
	}
}

func TestReceiverAcceptsGzipAndRejectsUnspecified(t *testing.T) {
	receiver := listen(t)
	var compressed bytes.Buffer
	writer := gzip.NewWriter(&compressed)
	if _, err := writer.Write(readGolden(t, "verdict.bin")); err != nil {
		t.Fatal(err)
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	request, err := http.NewRequest(http.MethodPost, receiver.LogsURL(), &compressed)
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("Content-Type", "application/x-protobuf")
	request.Header.Set("Content-Encoding", "gzip")
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	_, _ = io.Copy(io.Discard, response.Body)
	response.Body.Close()
	if response.StatusCode != http.StatusOK {
		t.Fatalf("gzip status = %d", response.StatusCode)
	}
	if len(receiver.Verdicts()) != 1 {
		t.Fatal("gzip verdict was not stored")
	}

	unspecified := decideExport(t, "req-unspecified", probev1.DecisionAction_DECISION_ACTION_UNSPECIFIED)
	postLogs(t, receiver.LogsURL(), unspecified, http.StatusBadRequest)
	if _, err := receiver.Response("req-unspecified"); err == nil {
		t.Fatal("an unspecified action must not be stored")
	}
}

func TestGateConsumesDecideResponseFromTheReceiver(t *testing.T) {
	receiver := listen(t)
	postLogs(t, receiver.LogsURL(), decideExport(t, "req-1", probev1.DecisionAction_DECISION_ACTION_PERMIT), http.StatusOK)

	probe := client.New(client.Config{SourceHandle: "sagashop-order"})
	probe.SetDecisions(receiver)
	outcome := enforcement.Gate(context.Background(), gateEvent(), gateFilter(), nil, enforcement.Deps{
		Decide:         probe.DecideFunc(),
		NowMonotonicNs: func() int64 { return 0 },
		AcceptedFailModeFor: func(*modelv1.SpecificationFilter) modelv1.FailMode {
			return modelv1.FailMode_FAIL_MODE_CLOSED
		},
	}, enforcement.Options{SourceHandle: "sagashop-order", RequestID: "req-1", IdempotencyKey: "idem-1"})
	if outcome.Kind != enforcement.OutcomePermit {
		t.Fatalf("kind = %v, want permit from the OTLP log", outcome.Kind)
	}

	postLogs(t, receiver.LogsURL(), decideExport(t, "req-2", probev1.DecisionAction_DECISION_ACTION_UNSPECIFIED), http.StatusBadRequest)
	denied := enforcement.Gate(context.Background(), gateEvent(), gateFilter(), nil, enforcement.Deps{
		Decide:         probe.DecideFunc(),
		NowMonotonicNs: func() int64 { return 0 },
		AcceptedFailModeFor: func(*modelv1.SpecificationFilter) modelv1.FailMode {
			return modelv1.FailMode_FAIL_MODE_CLOSED
		},
	}, enforcement.Options{SourceHandle: "sagashop-order", RequestID: "req-2", IdempotencyKey: "idem-2"})
	if denied.Kind != enforcement.OutcomeFailClosedDeny {
		t.Fatalf("kind = %v, want fail-closed when no specified decision arrived", denied.Kind)
	}
}

func listen(t *testing.T) *otlp.Receiver {
	t.Helper()
	receiver, err := otlp.Listen("127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = receiver.Close() })
	return receiver
}

func postLogs(t *testing.T, url string, body []byte, want int) {
	t.Helper()
	request, err := http.NewRequest(http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("Content-Type", "application/x-protobuf")
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	payload, _ := io.ReadAll(response.Body)
	if response.StatusCode != want {
		t.Fatalf("status = %d, want %d (%s)", response.StatusCode, want, payload)
	}
}

func decideExport(t *testing.T, requestID string, action probev1.DecisionAction) []byte {
	t.Helper()
	raw, err := proto.Marshal(&probev1.DecideResponse{RequestId: requestID, Action: action})
	if err != nil {
		t.Fatal(err)
	}
	body, err := proto.Marshal(&collogspb.ExportLogsServiceRequest{
		ResourceLogs: []*logspb.ResourceLogs{{
			ScopeLogs: []*logspb.ScopeLogs{{
				LogRecords: []*logspb.LogRecord{{
					Body: &commonpb.AnyValue{Value: &commonpb.AnyValue_BytesValue{BytesValue: raw}},
					Attributes: []*commonpb.KeyValue{
						{Key: otlp.AttributePayloadType, Value: &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: otlp.PayloadDecideResponse}}},
						{Key: otlp.AttributeRequestID, Value: &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: requestID}}},
					},
				}},
			}},
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	return body
}

func gateEvent() *modelv1.ProducerEvent {
	return &modelv1.ProducerEvent{Id: "evt-1", Kind: "transfer.initiated", SchemaVersion: "sentinel.model.v1"}
}

func gateFilter() *modelv1.EventFilter {
	epoch := uint64(4)
	budget := uint64(10_000)
	return &modelv1.EventFilter{
		Epoch: &epoch,
		Specifications: []*modelv1.SpecificationFilter{{
			SpecificationId:      "spec-1",
			SpecificationVersion: "1",
			EventMatch: &modelv1.EventMatch{
				EventKinds:   []string{"transfer.initiated"},
				DeliveryMode: modelv1.DeliveryMode_DELIVERY_MODE_ASK_AND_BLOCK,
			},
			FailMode:                 modelv1.FailMode_FAIL_MODE_CLOSED,
			EvaluationMode:           modelv1.EvaluationMode_EVALUATION_MODE_ENFORCE,
			Readiness:                modelv1.Readiness_READINESS_ACTIVE,
			LatencyBudgetNanoseconds: &budget,
		}},
	}
}

func TestListenWithDefaultsToLoopback(t *testing.T) {
	receiver, err := otlp.ListenWith(otlp.ListenConfig{})
	if err != nil {
		t.Fatal(err)
	}
	defer receiver.Close()
	if !strings.HasPrefix(receiver.LogsURL(), "http://127.0.0.1:") || receiver.Port() == 0 {
		t.Fatalf("url %q port %d", receiver.LogsURL(), receiver.Port())
	}
	if tcp, ok := receiver.Addr().(*net.TCPAddr); !ok || !tcp.IP.IsLoopback() {
		t.Fatalf("bound %v", receiver.Addr())
	}
}

// A containerised Probe binds every interface so Sentinel can reach it by container name.
func TestListenWithBindsAllInterfacesOnAChosenPort(t *testing.T) {
	probe, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	port := probe.Addr().(*net.TCPAddr).Port
	_ = probe.Close()

	receiver, err := otlp.ListenWith(otlp.ListenConfig{Host: "0.0.0.0", Port: port})
	if err != nil {
		t.Fatal(err)
	}
	defer receiver.Close()
	if receiver.Port() != port {
		t.Fatalf("port %d, want %d", receiver.Port(), port)
	}
	if tcp := receiver.Addr().(*net.TCPAddr); !tcp.IP.IsUnspecified() {
		t.Fatalf("bound %v, want the wildcard address", tcp)
	}
	if want := "http://sentinel-probe:" + strconv.Itoa(port) + "/v1/logs"; receiver.LogsURLFor("sentinel-probe") != want {
		t.Fatalf("LogsURLFor = %q, want %q", receiver.LogsURLFor("sentinel-probe"), want)
	}
	// Reachable on a non-loopback-named dial, here the loopback address of the wildcard bind.
	response, err := http.Post(receiver.LogsURL(), "text/plain", strings.NewReader("x"))
	if err != nil {
		t.Fatal(err)
	}
	_ = response.Body.Close()
	if response.StatusCode != http.StatusUnsupportedMediaType {
		t.Fatalf("status %d", response.StatusCode)
	}
}

func TestListenStillRefusesNonLoopback(t *testing.T) {
	if receiver, err := otlp.Listen("0.0.0.0:0"); err == nil {
		_ = receiver.Close()
		t.Fatal("Listen must keep refusing a non-loopback address")
	}
	if _, err := otlp.ListenWith(otlp.ListenConfig{Port: 70000}); err == nil {
		t.Fatal("an out-of-range port must be refused")
	}
}

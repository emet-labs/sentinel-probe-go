package otlp_test

import (
	"os"
	"path/filepath"
	"runtime"
	"testing"

	modelv1 "github.com/emet-labs/sentinel/sdk/go/gen/sentinel/model/v1"
	probev1 "github.com/emet-labs/sentinel/sdk/go/gen/sentinel/probe/v1"
	"github.com/emet-labs/sentinel/sdk/go/otlp"
	collogspb "go.opentelemetry.io/proto/otlp/collector/logs/v1"
	coltracepb "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	logspb "go.opentelemetry.io/proto/otlp/logs/v1"
	tracepb "go.opentelemetry.io/proto/otlp/trace/v1"
	"google.golang.org/protobuf/proto"
)

func otlpDir(t *testing.T) string {
	t.Helper()
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("runtime.Caller failed")
	}
	return filepath.Join(filepath.Dir(file), "..", "..", "..", "testdata", "otlp")
}

func readGolden(t *testing.T, name string) []byte {
	t.Helper()
	body, err := os.ReadFile(filepath.Join(otlpDir(t), name))
	if err != nil {
		t.Fatal(err)
	}
	return body
}

func TestGoldenChargeShip(t *testing.T) {
	var request coltracepb.ExportTraceServiceRequest
	if err := proto.Unmarshal(readGolden(t, "charge-ship.bin"), &request); err != nil {
		t.Fatal(err)
	}
	spans := allSpans(&request)
	if len(spans) != 2 {
		t.Fatalf("spans = %d, want 2", len(spans))
	}
	charge, ship := spans[0], spans[1]
	if charge.GetName() != "sagashop.payment.charge.completed" || ship.GetName() != "sagashop.order.ship.requested" {
		t.Fatalf("names = %q, %q", charge.GetName(), ship.GetName())
	}
	if attrString(t, charge.GetAttributes(), otlp.AttributeEventID) != "01f0c0ffee-charge" {
		t.Fatal("charge event id")
	}
	if _, ok := findAttr(ship.GetAttributes(), otlp.AttributeParentEventID); !ok {
		t.Fatal("ship predecessor must be sentinel.parent.event.id, not a span id")
	}
	if attrString(t, ship.GetAttributes(), otlp.AttributeParentEventID) != "01f0c0ffee-charge" {
		t.Fatal("ship predecessor")
	}
	for _, span := range spans {
		if attrInt(t, span.GetAttributes(), otlp.AttributeSequenceEpoch) != 7 {
			t.Fatal("sequence epoch")
		}
		if attrString(t, span.GetAttributes(), otlp.AttributeSchemaVersion) != "sagashop-contract-v1" {
			t.Fatal("schema")
		}
		if attrInt(t, span.GetAttributes(), otlp.AttributeAcknowledgedFilterEpoch) != 4 {
			t.Fatal("filter epoch")
		}
		if attrString(t, span.GetAttributes(), otlp.AttributeClaimedSensitivity) != "SENSITIVITY_INTERNAL" {
			t.Fatal("sensitivity")
		}
		if attrString(t, span.GetAttributes(), "sagashop.order.id") != "order-1001" {
			t.Fatal("order id")
		}
	}
	if attrInt(t, charge.GetAttributes(), otlp.AttributeSequenceNumber) != 1 ||
		attrInt(t, ship.GetAttributes(), otlp.AttributeSequenceNumber) != 2 {
		t.Fatal("sequence numbers")
	}
	if attrString(t, charge.GetAttributes(), "sagashop.payment.status") != "success" {
		t.Fatal("charge status")
	}
}

func TestGoldenLogs(t *testing.T) {
	completeness := readGolden(t, "completeness-order.bin")
	if _, err := otlp.DecodeInboundLogs(completeness); err == nil {
		t.Fatal("completeness is not an inbound Probe log")
	}
	assertCompleteness(t, completeness)

	verdict := decodeInbound(t, "verdict.bin")
	if len(verdict.Verdicts) != 1 || len(verdict.Filters) != 0 || len(verdict.Decisions) != 0 {
		t.Fatalf("verdict golden = %+v", verdict)
	}
	filter := decodeInbound(t, "event-filter.bin")
	if len(filter.Filters) != 1 || len(filter.Verdicts) != 0 || len(filter.Decisions) != 0 {
		t.Fatalf("filter golden = %+v", filter)
	}
	decision := decodeInbound(t, "decide-response.bin")
	if len(decision.Decisions) != 1 {
		t.Fatalf("decide golden = %+v", decision)
	}
	switch decision.Decisions[0].GetAction() {
	case probev1.DecisionAction_DECISION_ACTION_PERMIT,
		probev1.DecisionAction_DECISION_ACTION_DENY,
		probev1.DecisionAction_DECISION_ACTION_DEFER:
	default:
		t.Fatalf("decide golden action = %v", decision.Decisions[0].GetAction())
	}
}

func TestEncodeCompletenessMatchesGolden(t *testing.T) {
	last := uint64(2)
	epoch := uint64(4)
	when := &modelv1.OccurrenceTime{
		ClockDomainId: "unix",
		Nanoseconds:   &modelv1.Int128{Low: 1_700_000_000_000_000_000},
	}
	encoded, err := otlp.EncodeCompleteness(&modelv1.ProducerCompletenessSignal{
		SequenceEpoch:           7,
		LastEmitted:             &last,
		CompleteThrough:         &last,
		ProducerReportedAt:      when,
		AcknowledgedFilterEpoch: &epoch,
		Activity:                modelv1.SourceActivityState_SOURCE_ACTIVITY_STATE_ACTIVE,
		ActivitySince:           when,
	}, "sagashop-order")
	if err != nil {
		t.Fatal(err)
	}
	if !proto.Equal(unmarshalLogs(t, encoded), unmarshalLogs(t, readGolden(t, "completeness-order.bin"))) {
		t.Fatal("completeness export does not match completeness-order.bin")
	}
}

func assertCompleteness(t *testing.T, body []byte) {
	t.Helper()
	record := firstLog(t, unmarshalLogs(t, body))
	if attrText(t, record.GetAttributes(), otlp.AttributePayloadType) != otlp.PayloadCompleteness {
		t.Fatal("payload type")
	}
	if attrText(t, record.GetAttributes(), otlp.AttributeSourceHandle) != "sagashop-order" {
		t.Fatal("source handle")
	}
	if _, ok := findLogAttr(record.GetAttributes(), otlp.AttributeRequestID); ok {
		t.Fatal("completeness must not carry request id")
	}
	raw, ok := record.GetBody().GetValue().(*commonpb.AnyValue_BytesValue)
	if !ok {
		t.Fatal("completeness body must be bytes")
	}
	signal := &modelv1.ProducerCompletenessSignal{}
	if err := proto.Unmarshal(raw.BytesValue, signal); err != nil {
		t.Fatal(err)
	}
	if signal.GetSequenceEpoch() != 7 || signal.GetLastEmitted() != 2 || signal.GetCompleteThrough() != 2 {
		t.Fatalf("sequence bounds = %+v", signal)
	}
	if signal.GetAcknowledgedFilterEpoch() != 4 {
		t.Fatal("filter epoch")
	}
	if signal.GetProducerReportedAt().GetClockDomainId() != "unix" ||
		signal.GetProducerReportedAt().GetNanoseconds().GetLow() != 1_700_000_000_000_000_000 ||
		signal.GetProducerReportedAt().GetUncertaintyNanoseconds() != 0 {
		t.Fatal("occurrence")
	}
}

func decodeInbound(t *testing.T, name string) *otlp.Inbound {
	t.Helper()
	inbound, err := otlp.DecodeInboundLogs(readGolden(t, name))
	if err != nil {
		t.Fatalf("%s: %v", name, err)
	}
	return inbound
}

func unmarshalLogs(t *testing.T, body []byte) *collogspb.ExportLogsServiceRequest {
	t.Helper()
	var request collogspb.ExportLogsServiceRequest
	if err := proto.Unmarshal(body, &request); err != nil {
		t.Fatal(err)
	}
	return &request
}

func firstLog(t *testing.T, request *collogspb.ExportLogsServiceRequest) *logspb.LogRecord {
	t.Helper()
	records := request.GetResourceLogs()
	if len(records) != 1 || len(records[0].GetScopeLogs()) != 1 || len(records[0].GetScopeLogs()[0].GetLogRecords()) != 1 {
		t.Fatalf("log shape = %v", request)
	}
	return records[0].GetScopeLogs()[0].GetLogRecords()[0]
}

func allSpans(request *coltracepb.ExportTraceServiceRequest) []*tracepb.Span {
	var spans []*tracepb.Span
	for _, resource := range request.GetResourceSpans() {
		for _, scope := range resource.GetScopeSpans() {
			for _, span := range scope.GetSpans() {
				spans = append(spans, span)
			}
		}
	}
	return spans
}

func findAttr(attributes []*commonpb.KeyValue, key string) (*commonpb.AnyValue, bool) {
	for _, attribute := range attributes {
		if attribute.GetKey() == key {
			return attribute.GetValue(), true
		}
	}
	return nil, false
}

func attrString(t *testing.T, attributes []*commonpb.KeyValue, key string) string {
	t.Helper()
	value, ok := findAttr(attributes, key)
	if !ok {
		t.Fatalf("missing %s", key)
	}
	text, ok := value.GetValue().(*commonpb.AnyValue_StringValue)
	if !ok {
		t.Fatalf("%s is %T", key, value.GetValue())
	}
	return text.StringValue
}

func attrInt(t *testing.T, attributes []*commonpb.KeyValue, key string) int64 {
	t.Helper()
	value, ok := findAttr(attributes, key)
	if !ok {
		t.Fatalf("missing %s", key)
	}
	number, ok := value.GetValue().(*commonpb.AnyValue_IntValue)
	if !ok {
		t.Fatalf("%s is %T", key, value.GetValue())
	}
	return number.IntValue
}

func findLogAttr(attributes []*commonpb.KeyValue, key string) (*commonpb.AnyValue, bool) {
	return findAttr(attributes, key)
}

func attrText(t *testing.T, attributes []*commonpb.KeyValue, key string) string {
	t.Helper()
	return attrString(t, attributes, key)
}

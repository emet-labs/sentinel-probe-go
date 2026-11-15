package conformance

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"math/big"
	"os"
	"path/filepath"
	"regexp"
	"runtime"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/emet-labs/sentinel-probe-go/emission"
	"github.com/emet-labs/sentinel-probe-go/enforcement"
	modelv1 "github.com/emet-labs/sentinel-probe-go/gen/sentinel/model/v1"
	probev1 "github.com/emet-labs/sentinel-probe-go/gen/sentinel/probe/v1"
	int128codec "github.com/emet-labs/sentinel-probe-go/int128"
	"github.com/emet-labs/sentinel-probe-go/internal/specmatch"
	"github.com/emet-labs/sentinel-probe-go/otlp"
	"go.opentelemetry.io/otel/attribute"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"
	collogspb "go.opentelemetry.io/proto/otlp/collector/logs/v1"
	coltracepb "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	logspb "go.opentelemetry.io/proto/otlp/logs/v1"
	tracepb "go.opentelemetry.io/proto/otlp/trace/v1"
	"google.golang.org/protobuf/proto"
)

const version = "1.0.0"

type envelope struct {
	FormatVersion string          `json:"format_version"`
	Kind          string          `json:"kind"`
	Cases         json.RawMessage `json:"cases"`
}

type intCase struct {
	ID    string `json:"id"`
	Value string `json:"value"`
	High  string `json:"high"`
	Low   string `json:"low"`
}

type matchCase struct {
	ID                  string `json:"id"`
	SpecificationFilter struct {
		EventMatch *struct {
			EventKinds             []string `json:"event_kinds"`
			DeliveryMode           string   `json:"delivery_mode"`
			ProjectedAttributeKeys []string `json:"projected_attribute_keys"`
		} `json:"event_match"`
		FailMode string `json:"fail_mode"`
	} `json:"specification_filter"`
	ProducerEvent struct {
		Kind string `json:"kind"`
	} `json:"producer_event"`
	Expected bool `json:"expected"`
}

type gateCase struct {
	ID     string `json:"id"`
	Filter *struct {
		Epoch          string `json:"epoch"`
		Specifications []struct {
			ID               string   `json:"id"`
			EventKinds       []string `json:"event_kinds"`
			DeliveryMode     string   `json:"delivery_mode"`
			EvaluationMode   string   `json:"evaluation_mode"`
			Readiness        string   `json:"readiness"`
			LatencyBudgetNS  string   `json:"latency_budget_ns"`
			FailMode         string   `json:"fail_mode"`
			AcceptedFailMode string   `json:"accepted_fail_mode"`
		} `json:"specifications"`
	} `json:"filter"`
	Event struct {
		Kind string `json:"kind"`
	} `json:"event"`
	LocalDeadlineNS *string  `json:"local_deadline_ns"`
	ClockReadsNS    []string `json:"clock_reads_ns"`
	Decider         struct {
		Result string `json:"result"`
	} `json:"decider"`
	Expected struct {
		Kind                       string  `json:"kind"`
		Reason                     *string `json:"reason"`
		DecideCalls                int     `json:"decide_calls"`
		RemainingTransportBudgetNS *string `json:"remaining_transport_budget_ns"`
	} `json:"expected"`
}

func fixtureRoot(t *testing.T) string {
	t.Helper()
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("runtime.Caller failed")
	}
	return filepath.Clean(filepath.Join(filepath.Dir(file), "..", "..", "testdata", "probe-sdk-conformance"))
}

func decodeStrict(t *testing.T, path string, out any) {
	t.Helper()
	data, err := os.ReadFile(filepath.Join(fixtureRoot(t), path))
	if err != nil {
		t.Fatal(err)
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(out); err != nil {
		t.Fatalf("%s: %v", path, err)
	}
	if decoder.More() {
		t.Fatalf("%s: trailing JSON", path)
	}
}

func unique(t *testing.T, ids []string) {
	t.Helper()
	seen := make(map[string]struct{}, len(ids))
	for _, id := range ids {
		if id == "" {
			t.Fatal("empty case id")
		}
		if _, exists := seen[id]; exists {
			t.Fatalf("duplicate case id %q", id)
		}
		seen[id] = struct{}{}
	}
}

func TestInt128Vectors(t *testing.T) {
	var suite struct {
		FormatVersion string    `json:"format_version"`
		Kind          string    `json:"kind"`
		Cases         []intCase `json:"cases"`
	}
	decodeStrict(t, "int128-v1.json", &suite)
	if suite.FormatVersion != version || suite.Kind != "int128" {
		t.Fatal("unsupported suite")
	}
	ids := make([]string, 0, len(suite.Cases))
	for _, vector := range suite.Cases {
		ids = append(ids, vector.ID)
		value, ok := new(big.Int).SetString(vector.Value, 10)
		if !ok {
			t.Fatalf("%s: invalid value", vector.ID)
		}
		high, ok := new(big.Int).SetString(vector.High, 10)
		if !ok || !high.IsInt64() {
			t.Fatalf("%s: invalid high", vector.ID)
		}
		low, ok := new(big.Int).SetString(vector.Low, 10)
		if !ok || low.Sign() < 0 || low.BitLen() > 64 {
			t.Fatalf("%s: invalid low", vector.ID)
		}
		encoded := int128codec.FromBigInt(value)
		if encoded.High != high.Int64() || encoded.Low != low.Uint64() {
			t.Errorf("%s: encoded words = (%d,%d)", vector.ID, encoded.High, encoded.Low)
		}
		decoded := int128codec.ToBigInt(&modelv1.Int128{High: high.Int64(), Low: low.Uint64()})
		if decoded.Cmp(value) != 0 {
			t.Errorf("%s: decoded = %s, want %s", vector.ID, decoded, value)
		}
	}
	unique(t, ids)
}

func TestManifestSuiteRegistryFailsClosed(t *testing.T) {
	var manifest struct {
		FormatVersion string `json:"format_version"`
		Suites        []struct {
			Kind string `json:"kind"`
			Path string `json:"path"`
		} `json:"suites"`
		Malformed []json.RawMessage `json:"malformed"`
	}
	decodeStrict(t, "manifest-v1.json", &manifest)
	want := []string{"spec_match:spec-match-v1.json", "int128:int128-v1.json", "enforcement_gate:enforcement-gate-v1.json", "otlp_signal:otlp-signal-v1.json", "sequence_completeness:sequence-completeness-v1.json", "decision_wait:decision-wait-v1.json", "filter_store:filter-store-v1.json"}
	if manifest.FormatVersion != "1.4.0" || len(manifest.Suites) != len(want) {
		t.Fatal("unsupported manifest")
	}
	for index, entry := range manifest.Suites {
		if entry.Kind+":"+entry.Path != want[index] {
			t.Fatalf("unknown manifest suite at %d", index)
		}
	}
}

func TestMalformedCorpusRejectedByCategory(t *testing.T) {
	var manifest struct {
		FormatVersion string            `json:"format_version"`
		Suites        []json.RawMessage `json:"suites"`
		Malformed     []struct {
			Path     string `json:"path"`
			Category string `json:"rejection_category"`
		} `json:"malformed"`
	}
	decodeStrict(t, "manifest-v1.json", &manifest)
	for _, fixture := range manifest.Malformed {
		if got := classifyMalformed(t, fixture.Path); got != fixture.Category {
			t.Errorf("%s: category = %s, want %s", fixture.Path, got, fixture.Category)
		}
	}
}

func classifyMalformed(t *testing.T, path string) string {
	t.Helper()
	data, err := os.ReadFile(filepath.Join(fixtureRoot(t), path))
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Count(data, []byte(`"kind"`)) > 1 {
		return "duplicate-key"
	}
	var top map[string]json.RawMessage
	if err := json.Unmarshal(data, &top); err != nil {
		return "syntax"
	}
	if raw, ok := top["format_version"]; !ok {
		return "missing-field"
	} else {
		var v string
		_ = json.Unmarshal(raw, &v)
		if v != version {
			return "version"
		}
	}
	for key := range top {
		if key != "format_version" && key != "kind" && key != "cases" {
			return "unknown-field"
		}
	}
	if _, ok := top["cases"]; !ok {
		return "missing-field"
	}
	var kind string
	_ = json.Unmarshal(top["kind"], &kind)
	if kind != "int128" && kind != "spec_match" && kind != "enforcement_gate" {
		return "unknown-token"
	}
	if kind == "int128" {
		var cases []intCase
		if err := json.Unmarshal(top["cases"], &cases); err != nil {
			return "schema"
		}
		canonical := regexp.MustCompile(`^(0|-?[1-9][0-9]*)$`)
		seen := map[string]bool{}
		min, _ := new(big.Int).SetString("-170141183460469231731687303715884105728", 10)
		max, _ := new(big.Int).SetString("170141183460469231731687303715884105727", 10)
		for _, item := range cases {
			if seen[item.ID] {
				return "duplicate-id"
			}
			seen[item.ID] = true
			if !canonical.MatchString(item.Value) || !canonical.MatchString(item.High) || !canonical.MatchString(item.Low) {
				return "integer-lexeme"
			}
			value, _ := new(big.Int).SetString(item.Value, 10)
			if value.Cmp(min) < 0 || value.Cmp(max) > 0 {
				return "integer-range"
			}
		}
	}
	return "accepted"
}

func TestSpecMatchVectors(t *testing.T) {
	var suite struct {
		FormatVersion string      `json:"format_version"`
		Kind          string      `json:"kind"`
		Cases         []matchCase `json:"cases"`
	}
	decodeStrict(t, "spec-match-v1.json", &suite)
	if suite.FormatVersion != version || suite.Kind != "spec_match" {
		t.Fatal("unsupported suite")
	}
	ids := make([]string, 0, len(suite.Cases))
	for _, vector := range suite.Cases {
		ids = append(ids, vector.ID)
		spec := &modelv1.SpecificationFilter{}
		if vector.SpecificationFilter.EventMatch != nil {
			spec.EventMatch = &modelv1.EventMatch{
				EventKinds:             vector.SpecificationFilter.EventMatch.EventKinds,
				ProjectedAttributeKeys: vector.SpecificationFilter.EventMatch.ProjectedAttributeKeys,
			}
		}
		got := specmatch.Selects(spec, &modelv1.ProducerEvent{Kind: vector.ProducerEvent.Kind})
		if got != vector.Expected {
			t.Errorf("%s: Selects = %v, want %v", vector.ID, got, vector.Expected)
		}
	}
	unique(t, ids)
}

func TestEnforcementGateVectors(t *testing.T) {
	var suite struct {
		FormatVersion string     `json:"format_version"`
		Kind          string     `json:"kind"`
		Cases         []gateCase `json:"cases"`
	}
	decodeStrict(t, "enforcement-gate-v1.json", &suite)
	for _, vector := range suite.Cases {
		vector := vector
		t.Run(vector.ID, func(t *testing.T) {
			var filter *modelv1.EventFilter
			accepted := map[string]modelv1.FailMode{}
			if vector.Filter != nil {
				epoch, err := strconv.ParseUint(vector.Filter.Epoch, 10, 63)
				if err != nil {
					t.Fatal(err)
				}
				filter = &modelv1.EventFilter{Epoch: &epoch}
				for _, fixture := range vector.Filter.Specifications {
					failMode := modelv1.FailMode_FAIL_MODE_OPEN
					if fixture.FailMode == "closed" {
						failMode = modelv1.FailMode_FAIL_MODE_CLOSED
					}
					acceptedMode := modelv1.FailMode_FAIL_MODE_OPEN
					if fixture.AcceptedFailMode == "closed" {
						acceptedMode = modelv1.FailMode_FAIL_MODE_CLOSED
					}
					accepted[fixture.ID] = acceptedMode
					delivery := modelv1.DeliveryMode_DELIVERY_MODE_SHIP_ASYNC
					if fixture.DeliveryMode == "ask_and_block" {
						delivery = modelv1.DeliveryMode_DELIVERY_MODE_ASK_AND_BLOCK
					}
					budget, err := strconv.ParseUint(fixture.LatencyBudgetNS, 10, 64)
					if err != nil {
						t.Fatal(err)
					}
					filter.Specifications = append(filter.Specifications, &modelv1.SpecificationFilter{SpecificationId: fixture.ID, FailMode: failMode, EvaluationMode: modelv1.EvaluationMode_EVALUATION_MODE_ENFORCE, Readiness: modelv1.Readiness_READINESS_ACTIVE, LatencyBudgetNanoseconds: &budget, EventMatch: &modelv1.EventMatch{EventKinds: fixture.EventKinds, DeliveryMode: delivery}})
				}
			}
			reads := make([]int64, len(vector.ClockReadsNS))
			for index, raw := range vector.ClockReadsNS {
				value, err := strconv.ParseInt(raw, 10, 64)
				if err != nil {
					t.Fatal(err)
				}
				reads[index] = value
			}
			readIndex := 0
			requests := make([]*probev1.DecideRequest, 0, 1)
			deps := enforcement.Deps{
				NowMonotonicNs: func() int64 {
					if readIndex >= len(reads) {
						t.Fatal("clock script exhausted")
					}
					value := reads[readIndex]
					readIndex++
					return value
				},
				AcceptedFailModeFor: func(spec *modelv1.SpecificationFilter) modelv1.FailMode { return accepted[spec.GetSpecificationId()] },
				Decide: func(_ context.Context, request *probev1.DecideRequest) (*probev1.DecideResponse, error) {
					requests = append(requests, request)
					if vector.Decider.Result == "transport_error" {
						return nil, errors.New("fixture-transport-error")
					}
					actions := map[string]probev1.DecisionAction{"permit": probev1.DecisionAction_DECISION_ACTION_PERMIT, "deny": probev1.DecisionAction_DECISION_ACTION_DENY, "defer": probev1.DecisionAction_DECISION_ACTION_DEFER, "unspecified": probev1.DecisionAction_DECISION_ACTION_UNSPECIFIED}
					return &probev1.DecideResponse{Action: actions[vector.Decider.Result]}, nil
				},
			}
			var deadline *int64
			if vector.LocalDeadlineNS != nil {
				value, err := strconv.ParseInt(*vector.LocalDeadlineNS, 10, 64)
				if err != nil {
					t.Fatal(err)
				}
				deadline = &value
			}
			outcome := enforcement.Gate(context.Background(), &modelv1.ProducerEvent{Id: "fixture-event", Kind: vector.Event.Kind}, filter, deadline, deps, enforcement.Options{SourceHandle: "fixture-source", RequestID: "fixture-request", IdempotencyKey: "fixture-idempotency"})
			if outcome.Kind.String() != vector.Expected.Kind {
				t.Errorf("kind = %s, want %s", outcome.Kind, vector.Expected.Kind)
			}
			if len(requests) != vector.Expected.DecideCalls {
				t.Errorf("decide calls = %d, want %d", len(requests), vector.Expected.DecideCalls)
			}
			if readIndex != len(reads) {
				t.Errorf("clock reads = %d, want %d", readIndex, len(reads))
			}
			if len(requests) == 1 && vector.Expected.RemainingTransportBudgetNS != nil {
				want, _ := strconv.ParseUint(*vector.Expected.RemainingTransportBudgetNS, 10, 64)
				if requests[0].GetRemainingTransportBudgetNanoseconds() != want {
					t.Errorf("remaining budget mismatch")
				}
			}
		})
	}
}

type otlpCase struct {
	ID             string            `json:"id"`
	Signal         string            `json:"signal"`
	Path           string            `json:"path"`
	Reject         bool              `json:"reject"`
	Events         []otlpEvent       `json:"events"`
	Completeness   *otlpCompleteness `json:"completeness"`
	Verdict        *otlpVerdict      `json:"verdict"`
	EventFilter    *otlpFilter       `json:"event_filter"`
	DecideResponse *otlpDecide       `json:"decide_response"`
}

type otlpEvent struct {
	ID             string   `json:"id"`
	Kind           string   `json:"kind"`
	SequenceEpoch  string   `json:"sequence_epoch"`
	SequenceNumber string   `json:"sequence_number"`
	Predecessors   []string `json:"predecessors"`
}

type otlpCompleteness struct {
	SourceHandle            string `json:"source_handle"`
	SequenceEpoch           string `json:"sequence_epoch"`
	LastEmitted             string `json:"last_emitted"`
	CompleteThrough         string `json:"complete_through"`
	AcknowledgedFilterEpoch string `json:"acknowledged_filter_epoch"`
	ClockDomainID           string `json:"clock_domain_id"`
	OccurrenceNS            string `json:"occurrence_ns"`
	UncertaintyNanoseconds  string `json:"uncertainty_nanoseconds"`
	Activity                string `json:"activity"`
}

type otlpVerdict struct {
	ID      string `json:"id"`
	Outcome string `json:"outcome"`
}

type otlpFilter struct {
	Epoch            string   `json:"epoch"`
	SpecificationIDs []string `json:"specification_ids"`
}

type otlpDecide struct {
	RequestID      string `json:"request_id"`
	Action         string `json:"action"`
	Specifications []struct {
		ID     string `json:"id"`
		Action string `json:"action"`
	} `json:"specifications"`
}

func TestOtlpSignalVectors(t *testing.T) {
	var suite struct {
		FormatVersion string     `json:"format_version"`
		Kind          string     `json:"kind"`
		Cases         []otlpCase `json:"cases"`
	}
	decodeStrict(t, "otlp-signal-v1.json", &suite)
	if suite.FormatVersion != "1.1.0" || suite.Kind != "otlp_signal" {
		t.Fatal("unsupported otlp_signal suite")
	}
	ids := make([]string, 0, len(suite.Cases))
	for _, vector := range suite.Cases {
		ids = append(ids, vector.ID)
		body := readConformanceBytes(t, vector.Path)
		switch vector.Signal {
		case "trace":
			events, err := decodeTraceGoldens(body)
			if vector.Reject {
				if err == nil {
					t.Fatalf("%s: golden reject was accepted", vector.ID)
				}
				continue
			}
			if err != nil {
				t.Fatalf("%s: %v", vector.ID, err)
			}
			if len(events) != len(vector.Events) {
				t.Fatalf("%s: events = %d", vector.ID, len(events))
			}
			for index, event := range events {
				want := vector.Events[index]
				if event.GetId() != want.ID || event.GetKind() != want.Kind {
					t.Fatalf("%s: event = %s %s", vector.ID, event.GetId(), event.GetKind())
				}
				if strconv.FormatUint(event.GetSequence().GetEpoch(), 10) != want.SequenceEpoch ||
					strconv.FormatUint(event.GetSequence().GetSequence(), 10) != want.SequenceNumber {
					t.Fatalf("%s: sequence", vector.ID)
				}
				if !sameStrings(event.GetCausalPredecessorIds(), want.Predecessors) {
					t.Fatalf("%s: predecessors = %v", vector.ID, event.GetCausalPredecessorIds())
				}
			}
		case "completeness":
			assertCompletenessGolden(t, vector.ID, body, vector.Completeness)
		case "verdict":
			inbound := mustInbound(t, body)
			if len(inbound.Verdicts) != 1 || inbound.Verdicts[0].GetId() != vector.Verdict.ID || outcomeToken(inbound.Verdicts[0].GetOutcome()) != vector.Verdict.Outcome {
				t.Fatalf("%s: verdict", vector.ID)
			}
		case "event_filter":
			inbound := mustInbound(t, body)
			if len(inbound.Filters) != 1 || strconv.FormatUint(inbound.Filters[0].GetEpoch(), 10) != vector.EventFilter.Epoch || !sameStrings(specIDs(inbound.Filters[0]), vector.EventFilter.SpecificationIDs) {
				t.Fatalf("%s: event filter", vector.ID)
			}
		case "decide_response":
			inbound := mustInbound(t, body)
			if len(inbound.Decisions) != 1 {
				t.Fatalf("%s: decisions", vector.ID)
			}
			decision := inbound.Decisions[0]
			if decision.GetRequestId() != vector.DecideResponse.RequestID || actionToken(decision.GetAction()) != vector.DecideResponse.Action {
				t.Fatalf("%s: decide", vector.ID)
			}
			if len(decision.GetSpecifications()) != len(vector.DecideResponse.Specifications) {
				t.Fatalf("%s: specification decisions", vector.ID)
			}
			for index, spec := range decision.GetSpecifications() {
				want := vector.DecideResponse.Specifications[index]
				if spec.GetSpecificationId() != want.ID || actionToken(spec.GetAction()) != want.Action {
					t.Fatalf("%s: specification %d", vector.ID, index)
				}
			}
		default:
			t.Fatalf("unknown signal %s", vector.Signal)
		}
	}
	unique(t, ids)
}

func readConformanceBytes(t *testing.T, relative string) []byte {
	t.Helper()
	path := filepath.Clean(filepath.Join(fixtureRoot(t), filepath.FromSlash(relative)))
	otlpDir := filepath.Clean(filepath.Join(fixtureRoot(t), "..", "otlp"))
	if !strings.HasPrefix(path, otlpDir+string(os.PathSeparator)) {
		t.Fatalf("path escapes otlp fixtures: %s", relative)
	}
	body, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return body
}

func decodeTraceGoldens(body []byte) ([]*modelv1.ProducerEvent, error) {
	var request coltracepb.ExportTraceServiceRequest
	if err := proto.Unmarshal(body, &request); err != nil {
		return nil, err
	}
	var spans []sdktrace.ReadOnlySpan
	var events []*modelv1.ProducerEvent
	for _, span := range protoSpans(&request) {
		stub, err := stubFromProto(span)
		if err != nil {
			return nil, err
		}
		snapshot := stub.Snapshot()
		spans = append(spans, snapshot)
		events = append(events, emission.SpanToEvent(conversionFrom(snapshot)))
	}
	if _, err := otlp.BuildExport(spans); err != nil {
		return nil, err
	}
	return events, nil
}

func protoSpans(request *coltracepb.ExportTraceServiceRequest) []*tracepb.Span {
	var spans []*tracepb.Span
	for _, resource := range request.GetResourceSpans() {
		for _, scope := range resource.GetScopeSpans() {
			spans = append(spans, scope.GetSpans()...)
		}
	}
	return spans
}

func stubFromProto(span *tracepb.Span) (tracetest.SpanStub, error) {
	attrs := make([]attribute.KeyValue, 0, len(span.GetAttributes()))
	for _, kv := range span.GetAttributes() {
		encoded, err := keyValueFromProto(kv)
		if err != nil {
			return tracetest.SpanStub{}, err
		}
		attrs = append(attrs, encoded)
	}
	stub := tracetest.SpanStub{
		Name:       span.GetName(),
		StartTime:  time.Unix(0, int64(span.GetStartTimeUnixNano())).UTC(),
		EndTime:    time.Unix(0, int64(span.GetEndTimeUnixNano())).UTC(),
		Attributes: attrs,
	}
	if parent := span.GetParentSpanId(); len(parent) == 8 && !allZero(parent) && len(span.GetTraceId()) == 16 {
		var traceID trace.TraceID
		var spanID trace.SpanID
		copy(traceID[:], span.GetTraceId())
		copy(spanID[:], parent)
		stub.Parent = trace.NewSpanContext(trace.SpanContextConfig{TraceID: traceID, SpanID: spanID})
	}
	return stub, nil
}

func keyValueFromProto(kv *commonpb.KeyValue) (attribute.KeyValue, error) {
	switch value := kv.GetValue().GetValue().(type) {
	case *commonpb.AnyValue_StringValue:
		return attribute.String(kv.GetKey(), value.StringValue), nil
	case *commonpb.AnyValue_IntValue:
		return attribute.Int64(kv.GetKey(), value.IntValue), nil
	case *commonpb.AnyValue_BoolValue:
		return attribute.Bool(kv.GetKey(), value.BoolValue), nil
	case *commonpb.AnyValue_DoubleValue:
		return attribute.Float64(kv.GetKey(), value.DoubleValue), nil
	default:
		return attribute.KeyValue{}, errors.New("unsupported otlp attribute " + kv.GetKey())
	}
}

func conversionFrom(span sdktrace.ReadOnlySpan) emission.SpanConversion {
	attrs := span.Attributes()
	epoch := uintAttr(attrs, emission.AttributeSequenceEpoch)
	number := uintAttr(attrs, emission.AttributeSequenceNumber)
	filter := uintAttr(attrs, emission.AttributeAcknowledgedFilterEpoch)
	schema, _ := stringAttr(attrs, emission.AttributeSchemaVersion)
	eventID, _ := stringAttr(attrs, emission.AttributeEventID)
	return emission.SpanConversion{
		Span:              span,
		Sequence:          &modelv1.SequenceCoordinate{Epoch: epoch, Sequence: number},
		SchemaVersion:     schema,
		AcknowledgedEpoch: &filter,
		EventID:           eventID,
	}
}

func uintAttr(attrs []attribute.KeyValue, key string) uint64 {
	for _, kv := range attrs {
		if string(kv.Key) == key && kv.Value.Type() == attribute.INT64 && kv.Value.AsInt64() >= 0 {
			return uint64(kv.Value.AsInt64())
		}
	}
	return 0
}

func stringAttr(attrs []attribute.KeyValue, key string) (string, bool) {
	for _, kv := range attrs {
		if string(kv.Key) == key && kv.Value.Type() == attribute.STRING {
			return kv.Value.AsString(), true
		}
	}
	return "", false
}

func assertCompletenessGolden(t *testing.T, id string, body []byte, want *otlpCompleteness) {
	t.Helper()
	if want == nil {
		t.Fatalf("%s: missing completeness", id)
	}
	record := firstLogRecord(t, body)
	if textAttr(record, otlp.AttributePayloadType) != otlp.PayloadCompleteness || textAttr(record, otlp.AttributeSourceHandle) != want.SourceHandle {
		t.Fatalf("%s: completeness envelope", id)
	}
	raw, ok := record.GetBody().GetValue().(*commonpb.AnyValue_BytesValue)
	if !ok {
		t.Fatalf("%s: completeness body", id)
	}
	signal := &modelv1.ProducerCompletenessSignal{}
	if err := proto.Unmarshal(raw.BytesValue, signal); err != nil {
		t.Fatal(err)
	}
	if strconv.FormatUint(signal.GetSequenceEpoch(), 10) != want.SequenceEpoch ||
		strconv.FormatUint(signal.GetLastEmitted(), 10) != want.LastEmitted ||
		strconv.FormatUint(signal.GetCompleteThrough(), 10) != want.CompleteThrough ||
		strconv.FormatUint(signal.GetAcknowledgedFilterEpoch(), 10) != want.AcknowledgedFilterEpoch {
		t.Fatalf("%s: completeness sequence", id)
	}
	when := signal.GetProducerReportedAt()
	got := int128codec.ToBigInt(when.GetNanoseconds())
	occurrence, ok := new(big.Int).SetString(want.OccurrenceNS, 10)
	if !ok || got.Cmp(occurrence) != 0 || when.GetClockDomainId() != want.ClockDomainID || strconv.FormatUint(when.GetUncertaintyNanoseconds(), 10) != want.UncertaintyNanoseconds {
		t.Fatalf("%s: occurrence", id)
	}
	if activityToken(signal.GetActivity()) != want.Activity {
		t.Fatalf("%s: activity %s", id, signal.GetActivity())
	}
}

func firstLogRecord(t *testing.T, body []byte) *logspb.LogRecord {
	t.Helper()
	var request collogspb.ExportLogsServiceRequest
	if err := proto.Unmarshal(body, &request); err != nil {
		t.Fatal(err)
	}
	resources := request.GetResourceLogs()
	if len(resources) != 1 || len(resources[0].GetScopeLogs()) != 1 || len(resources[0].GetScopeLogs()[0].GetLogRecords()) != 1 {
		t.Fatal("log golden shape")
	}
	return resources[0].GetScopeLogs()[0].GetLogRecords()[0]
}

func textAttr(record *logspb.LogRecord, key string) string {
	for _, attribute := range record.GetAttributes() {
		if attribute.GetKey() != key {
			continue
		}
		if text, ok := attribute.GetValue().GetValue().(*commonpb.AnyValue_StringValue); ok {
			return text.StringValue
		}
	}
	return ""
}

func mustInbound(t *testing.T, body []byte) *otlp.Inbound {
	t.Helper()
	inbound, err := otlp.DecodeInboundLogs(body)
	if err != nil {
		t.Fatal(err)
	}
	return inbound
}

func specIDs(filter *modelv1.EventFilter) []string {
	ids := make([]string, 0, len(filter.GetSpecifications()))
	for _, spec := range filter.GetSpecifications() {
		ids = append(ids, spec.GetSpecificationId())
	}
	return ids
}

func outcomeToken(outcome modelv1.VerdictOutcome) string {
	switch outcome {
	case modelv1.VerdictOutcome_VERDICT_OUTCOME_SATISFY:
		return "satisfy"
	case modelv1.VerdictOutcome_VERDICT_OUTCOME_VIOLATE:
		return "violate"
	case modelv1.VerdictOutcome_VERDICT_OUTCOME_INCONCLUSIVE:
		return "inconclusive"
	default:
		return ""
	}
}

func actionToken(action probev1.DecisionAction) string {
	switch action {
	case probev1.DecisionAction_DECISION_ACTION_PERMIT:
		return "permit"
	case probev1.DecisionAction_DECISION_ACTION_DENY:
		return "deny"
	case probev1.DecisionAction_DECISION_ACTION_DEFER:
		return "defer"
	default:
		return ""
	}
}

func activityToken(activity modelv1.SourceActivityState) string {
	switch activity {
	case modelv1.SourceActivityState_SOURCE_ACTIVITY_STATE_ACTIVE:
		return "active"
	case modelv1.SourceActivityState_SOURCE_ACTIVITY_STATE_INTENTIONALLY_IDLE:
		return "idle"
	default:
		return ""
	}
}

func sameStrings(got, want []string) bool {
	if len(got) != len(want) {
		return false
	}
	for index := range got {
		if got[index] != want[index] {
			return false
		}
	}
	return true
}

func allZero(value []byte) bool {
	for _, byte := range value {
		if byte != 0 {
			return false
		}
	}
	return true
}

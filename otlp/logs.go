package otlp

import (
	"bytes"
	"context"
	"fmt"
	"math"
	"strings"

	modelv1 "github.com/emet-labs/sentinel/sdk/go/gen/sentinel/model/v1"
	probev1 "github.com/emet-labs/sentinel/sdk/go/gen/sentinel/probe/v1"
	collogspb "go.opentelemetry.io/proto/otlp/collector/logs/v1"
	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	logspb "go.opentelemetry.io/proto/otlp/logs/v1"
	"google.golang.org/protobuf/proto"
)

// Inbound is one decoded OTLP log export addressed to the Probe.
// Completeness is not inbound. An empty export is zero messages, not a pass.
type Inbound struct {
	Verdicts  []*modelv1.Verdict
	Filters   []*modelv1.EventFilter
	Decisions []*probev1.DecideResponse
}

// EncodeCompleteness encodes one producer completeness signal as an OTLP log
// export. The body is the signal's protobuf. sentinel.payload.type is
// sentinel.model.v1.ProducerCompletenessSignal and sentinel.source_handle is
// the deployment handle the caller also sends as x-sentinel-source-handle.
func EncodeCompleteness(signal *modelv1.ProducerCompletenessSignal, sourceHandle string) ([]byte, error) {
	if signal == nil {
		return nil, refuse("missing-completeness", "")
	}
	if strings.TrimSpace(sourceHandle) == "" {
		return nil, refuse("missing-source-handle", "")
	}
	body, err := proto.Marshal(signal)
	if err != nil {
		return nil, fmt.Errorf("otlp: marshal completeness: %w", err)
	}
	return encodeLog(PayloadCompleteness, sourceHandle, "", body)
}

// ExportCompleteness posts one completeness log to endpoint/v1/logs.
func ExportCompleteness(ctx context.Context, endpoint, sourceHandle string, signal *modelv1.ProducerCompletenessSignal) error {
	body, err := EncodeCompleteness(signal, sourceHandle)
	if err != nil {
		return err
	}
	url, err := endpointURL(endpoint, "/v1/logs")
	if err != nil {
		return err
	}
	return postProtobuf(ctx, nil, url, sourceHandle, body)
}

// DecodeInboundLogs decodes an uncompressed ExportLogsServiceRequest into
// Verdict, EventFilter, and DecideResponse messages. A non-bytes body, a
// payload type other than those three, any other sentinel.* attribute, or an
// unspecified decision action rejects the whole export and returns nothing.
func DecodeInboundLogs(body []byte) (*Inbound, error) {
	if len(body) == 0 {
		return &Inbound{}, nil
	}
	var request collogspb.ExportLogsServiceRequest
	if err := proto.Unmarshal(body, &request); err != nil {
		return nil, refuse("malformed-protobuf", err.Error())
	}
	inbound := &Inbound{}
	for _, resource := range request.GetResourceLogs() {
		if carried := resource.GetResource(); carried != nil {
			if err := rejectCarriedLog(carried.GetDroppedAttributesCount(), carried.GetAttributes()); err != nil {
				return nil, err
			}
		}
		for _, scope := range resource.GetScopeLogs() {
			if carried := scope.GetScope(); carried != nil {
				if err := rejectCarriedLog(carried.GetDroppedAttributesCount(), carried.GetAttributes()); err != nil {
					return nil, err
				}
			}
			for _, record := range scope.GetLogRecords() {
				if err := decodeRecord(inbound, record); err != nil {
					return nil, err
				}
			}
		}
	}
	return inbound, nil
}

func encodeLog(payloadType, sourceHandle, requestID string, body []byte) ([]byte, error) {
	attributes := []*commonpb.KeyValue{stringKV(AttributePayloadType, payloadType)}
	if sourceHandle != "" {
		attributes = append(attributes, stringKV(AttributeSourceHandle, sourceHandle))
	}
	if requestID != "" {
		attributes = append(attributes, stringKV(AttributeRequestID, requestID))
	}
	request := &collogspb.ExportLogsServiceRequest{
		ResourceLogs: []*logspb.ResourceLogs{{
			ScopeLogs: []*logspb.ScopeLogs{{
				LogRecords: []*logspb.LogRecord{{
					Body:       &commonpb.AnyValue{Value: &commonpb.AnyValue_BytesValue{BytesValue: body}},
					Attributes: attributes,
				}},
			}},
		}},
	}
	return proto.Marshal(request)
}

func decodeRecord(inbound *Inbound, record *logspb.LogRecord) error {
	if record.GetDroppedAttributesCount() != 0 {
		return refuse("dropped-attributes", "log record")
	}
	payloadType, requestID, err := logAttributes(record.GetAttributes())
	if err != nil {
		return err
	}
	raw, err := logBody(record.GetBody())
	if err != nil {
		return err
	}
	switch payloadType {
	case PayloadVerdict:
		if requestID != "" {
			return refuse("unexpected-request-id", PayloadVerdict)
		}
		verdict := &modelv1.Verdict{}
		if err := proto.Unmarshal(raw, verdict); err != nil {
			return refuse("malformed-protobuf", err.Error())
		}
		inbound.Verdicts = append(inbound.Verdicts, verdict)
	case PayloadEventFilter:
		if requestID != "" {
			return refuse("unexpected-request-id", PayloadEventFilter)
		}
		filter := &modelv1.EventFilter{}
		if err := proto.Unmarshal(raw, filter); err != nil {
			return refuse("malformed-protobuf", err.Error())
		}
		inbound.Filters = append(inbound.Filters, filter)
	case PayloadDecideResponse:
		decision, err := decodeDecision(raw, requestID)
		if err != nil {
			return err
		}
		inbound.Decisions = append(inbound.Decisions, decision)
	case "":
		return refuse("missing-payload-type", AttributePayloadType)
	default:
		return refuse("unexpected-payload-type", payloadType)
	}
	return nil
}

func decodeDecision(raw []byte, requestID string) (*probev1.DecideResponse, error) {
	decision := &probev1.DecideResponse{}
	if err := proto.Unmarshal(raw, decision); err != nil {
		return nil, refuse("malformed-protobuf", err.Error())
	}
	if len(decision.ProtoReflect().GetUnknown()) != 0 {
		return nil, refuse("unread-fields", PayloadDecideResponse)
	}
	if strings.TrimSpace(decision.GetRequestId()) == "" {
		return nil, refuse("empty-request-id", "")
	}
	if requestID == "" {
		return nil, refuse("missing-request-id", AttributeRequestID)
	}
	if requestID != decision.GetRequestId() {
		return nil, refuse("request-id-mismatch", requestID)
	}
	if err := requireSpecifiedAction(decision.GetAction(), "action"); err != nil {
		return nil, err
	}
	for i, spec := range decision.GetSpecifications() {
		if err := requireSpecifiedAction(spec.GetAction(), fmt.Sprintf("specification %d", i)); err != nil {
			return nil, err
		}
	}
	return decision, nil
}

func requireSpecifiedAction(action probev1.DecisionAction, where string) error {
	switch action {
	case probev1.DecisionAction_DECISION_ACTION_PERMIT,
		probev1.DecisionAction_DECISION_ACTION_DENY,
		probev1.DecisionAction_DECISION_ACTION_DEFER:
		return nil
	case probev1.DecisionAction_DECISION_ACTION_UNSPECIFIED:
		return refuse("unspecified-action", where)
	default:
		return refuse("unknown-action", where)
	}
}

func logAttributes(attributes []*commonpb.KeyValue) (string, string, error) {
	var payloadType, requestID string
	seen := make(map[string]struct{}, len(attributes))
	for _, attribute := range attributes {
		key := attribute.GetKey()
		if _, ok := seen[key]; ok {
			return "", "", refuse("duplicate-attribute", key)
		}
		seen[key] = struct{}{}
		if err := rejectNonFinite(attribute.GetValue()); err != nil {
			return "", "", err
		}
		if !isSentinel(key) {
			continue
		}
		text, ok := attribute.GetValue().GetValue().(*commonpb.AnyValue_StringValue)
		if !ok {
			return "", "", refuse("invalid-sentinel-attribute", key)
		}
		switch key {
		case AttributePayloadType:
			payloadType = text.StringValue
		case AttributeRequestID:
			requestID = text.StringValue
		default:
			return "", "", refuse("unknown-sentinel-attribute", key)
		}
	}
	return payloadType, requestID, nil
}

func logBody(body *commonpb.AnyValue) ([]byte, error) {
	raw, ok := body.GetValue().(*commonpb.AnyValue_BytesValue)
	if !ok || raw == nil {
		return nil, refuse("non-bytes-body", "")
	}
	return bytes.Clone(raw.BytesValue), nil
}

func rejectCarriedLog(dropped uint32, attributes []*commonpb.KeyValue) error {
	if dropped != 0 {
		return refuse("dropped-attributes", "resource or scope")
	}
	for _, attribute := range attributes {
		if err := rejectNonFinite(attribute.GetValue()); err != nil {
			return err
		}
	}
	return nil
}

func rejectNonFinite(value *commonpb.AnyValue) error {
	if value == nil || value.GetValue() == nil {
		return nil
	}
	switch typed := value.GetValue().(type) {
	case *commonpb.AnyValue_DoubleValue:
		if err := finite(typed.DoubleValue); err != nil {
			return err
		}
	case *commonpb.AnyValue_ArrayValue:
		for _, item := range typed.ArrayValue.GetValues() {
			if err := rejectNonFinite(item); err != nil {
				return err
			}
		}
	case *commonpb.AnyValue_KvlistValue:
		for _, entry := range typed.KvlistValue.GetValues() {
			if err := rejectNonFinite(entry.GetValue()); err != nil {
				return err
			}
		}
	}
	return nil
}

func finite(number float64) error {
	if math.IsNaN(number) || math.IsInf(number, 0) {
		return refuse("non-finite-double", "")
	}
	return nil
}

func stringKV(key, value string) *commonpb.KeyValue {
	return &commonpb.KeyValue{Key: key, Value: stringAny(value)}
}

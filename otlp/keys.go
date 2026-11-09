package otlp

// Span attribute keys the #398 trace contract requires. Causal ids are event
// ids. A span id is never written into one of these keys.
const (
	AttributeEventID                 = "sentinel.event.id"
	AttributeParentEventID           = "sentinel.parent.event.id"
	AttributeSequenceEpoch           = "sentinel.sequence.epoch"
	AttributeSequenceNumber          = "sentinel.sequence.number"
	AttributeSchemaVersion           = "sentinel.schema.version"
	AttributeAcknowledgedFilterEpoch = "sentinel.acknowledged_filter_epoch"
	AttributeClaimedSensitivity      = "sentinel.claimed_sensitivity"
	AttributeClaimedCapabilities     = "sentinel.claimed_capabilities"
)

// Log-record attribute keys. sentinel.request_id is present only on a
// DecideResponse. sentinel.source_handle is present only on completeness.
const (
	AttributePayloadType  = "sentinel.payload.type"
	AttributeRequestID    = "sentinel.request_id"
	AttributeSourceHandle = "sentinel.source_handle"
)

// Payload types carried in an OTLP log body.
const (
	PayloadCompleteness   = "sentinel.model.v1.ProducerCompletenessSignal"
	PayloadEventFilter    = "sentinel.model.v1.EventFilter"
	PayloadVerdict        = "sentinel.model.v1.Verdict"
	PayloadDecideResponse = "sentinel.probe.v1.DecideResponse"
)

// SourceHandleHeader is the deployment handle on an OTLP/HTTP request.
const SourceHandleHeader = "x-sentinel-source-handle"

const protobufMediaType = "application/x-protobuf"

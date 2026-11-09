// Package otlp is the Probe wire for the Go SDK (#405, the #398 exception to ADR-0002).
//
// Ended spans are posted as OTLP/HTTP protobuf to Sentinel's /v1/traces, with
// x-sentinel-source-handle. A predecessor that would have been a span id is not
// exported. Producer completeness, when the SDK sends it, is an OTLP log whose
// payload type is sentinel.model.v1.ProducerCompletenessSignal.
//
// The localhost receiver serves POST /v1/logs and decodes Verdict, EventFilter,
// and DecideResponse. Unspecified decision actions are rejected. The enforcement
// gate reads DecideResponse from that receiver. This package does not dial
// SentinelDecisionService.
package otlp

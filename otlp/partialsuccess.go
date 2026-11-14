package otlp

import (
	"fmt"

	coltracepb "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	"google.golang.org/protobuf/proto"
)

// RejectedSpans returns ExportTraceServiceResponse.partial_success.rejected_spans
// from an OTLP trace export response body (#461).
//
// An empty body, or one without partial_success, rejects nothing. Sentinel
// counts conflicts, admission refusals and Events above a closed boundary as
// rejected; a duplicate is reported with zero rejected spans and a message.
// An export succeeded only on HTTP 2xx with zero rejected spans, so an error
// here (an undecodable body, or a negative count) is an export failure.
func RejectedSpans(body []byte) (int64, error) {
	rejected, _, err := PartialSuccess(body)
	return rejected, err
}

// PartialSuccess is RejectedSpans that also returns partial_success.error_message.
func PartialSuccess(body []byte) (rejected int64, message string, err error) {
	var response coltracepb.ExportTraceServiceResponse
	if err := proto.Unmarshal(body, &response); err != nil {
		return 0, "", fmt.Errorf("otlp: undecodable trace export response: %w", err)
	}
	rejected = response.GetPartialSuccess().GetRejectedSpans()
	if rejected < 0 {
		return 0, "", refuse("negative-rejected-spans", fmt.Sprint(rejected))
	}
	return rejected, response.GetPartialSuccess().GetErrorMessage(), nil
}

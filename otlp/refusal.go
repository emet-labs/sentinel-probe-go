package otlp

import (
	"errors"
	"fmt"
)

// Refusal is why an ended span or log record was not put on the wire.
// A missing predecessor is a Refusal. The span id is not substituted.
type Refusal struct {
	Reason string
	Detail string
}

func (r *Refusal) Error() string {
	if r.Detail == "" {
		return "otlp: " + r.Reason
	}
	return fmt.Sprintf("otlp: %s: %s", r.Reason, r.Detail)
}

func refuse(reason, detail string) error {
	return &Refusal{Reason: reason, Detail: detail}
}

// DecisionRefusal is a Refusal of a DecideResponse log whose request id was
// already read and checked, for example one with an unspecified action. The
// receiver records it for RequestID, so a waiting ask for that id fails at once
// rather than waiting out its budget.
type DecisionRefusal struct {
	Refusal
	// RequestID is the DecideResponse request id the refusal belongs to.
	RequestID string
}

// Unwrap exposes the embedded Refusal to errors.As.
func (r *DecisionRefusal) Unwrap() error { return &r.Refusal }

func refuseDecision(requestID string, err error) error {
	var refusal *Refusal
	if !errors.As(err, &refusal) {
		return err
	}
	return &DecisionRefusal{Refusal: *refusal, RequestID: requestID}
}

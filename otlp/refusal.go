package otlp

import "fmt"

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

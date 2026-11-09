package otlp

import "time"

// exportTimeout bounds one OTLP POST. It is not an evidence clock.
const exportTimeout = 5 * time.Second

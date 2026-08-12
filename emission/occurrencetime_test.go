package emission_test

import (
	"math/big"
	"testing"
	"time"

	"go.opentelemetry.io/otel/sdk/trace/tracetest"

	"github.com/emet-labs/sentinel/sdk/go/emission"
	"github.com/emet-labs/sentinel/sdk/go/int128"
)

func TestOccurrenceTimeUsesUnixClockDomainAndZeroUncertainty(t *testing.T) {
	t.Parallel()

	occurrence := emission.BuildOccurrenceTime(snapshot(tracetest.SpanStub{
		Name:      "x",
		StartTime: time.Unix(1700000000, 123456789),
	}))

	if occurrence.GetClockDomainId() != "unix" {
		t.Errorf("ClockDomainId = %q, want unix", occurrence.GetClockDomainId())
	}
	// Always zero, mirroring span-to-event.ts:97-103 exactly. There is no Probe-side input
	// for SOURCE_CAPABILITY_BOUNDED_CLOCK_UNCERTAINTY and SpanConversion deliberately
	// carries no field to supply one.
	if occurrence.GetUncertaintyNanoseconds() != 0 {
		t.Errorf("UncertaintyNanoseconds = %d, want 0", occurrence.GetUncertaintyNanoseconds())
	}
	want := big.NewInt(1700000000123456789)
	if got := int128.ToBigInt(occurrence.GetNanoseconds()); got.Cmp(want) != 0 {
		t.Errorf("nanoseconds = %s, want %s", got, want)
	}
}

func TestOccurrenceTimeComesFromStartNotEndTime(t *testing.T) {
	t.Parallel()

	occurrence := emission.BuildOccurrenceTime(snapshot(tracetest.SpanStub{
		Name:      "x",
		StartTime: time.Unix(1700000000, 0),
		EndTime:   time.Unix(1700000009, 0),
	}))
	if got := int128.ToBigInt(occurrence.GetNanoseconds()); got.Cmp(big.NewInt(1700000000000000000)) != 0 {
		t.Fatalf("nanoseconds = %s, want the START time", got)
	}
}

func TestOccurrenceTimeSurvivesBeyondInt64Nanoseconds(t *testing.T) {
	t.Parallel()

	// UnixNano() is undefined outside 1678-2262. This instant is outside it, so the value
	// must span both Int128 words rather than wrap.
	moment := time.Date(3000, time.January, 1, 0, 0, 0, 0, time.UTC)
	occurrence := emission.BuildOccurrenceTime(snapshot(tracetest.SpanStub{Name: "x", StartTime: moment}))

	if occurrence.GetNanoseconds().GetHigh() == 0 {
		t.Fatal("an instant beyond 2^64 nanoseconds must use the high word")
	}
	want := int128.TimeToNanoseconds(moment)
	if got := int128.ToBigInt(occurrence.GetNanoseconds()); got.Cmp(want) != 0 {
		t.Fatalf("nanoseconds = %s, want %s", got, want)
	}
}

func TestOccurrenceTimeBeforeTheEpoch(t *testing.T) {
	t.Parallel()

	occurrence := emission.BuildOccurrenceTime(snapshot(tracetest.SpanStub{
		Name:      "x",
		StartTime: time.Unix(-1, 500000000),
	}))
	want := big.NewInt(-500000000)
	if got := int128.ToBigInt(occurrence.GetNanoseconds()); got.Cmp(want) != 0 {
		t.Fatalf("nanoseconds = %s, want %s (sign extension, not a wrapped unsigned word)", got, want)
	}
	if occurrence.GetNanoseconds().GetHigh() != -1 {
		t.Fatalf("high = %d, want -1", occurrence.GetNanoseconds().GetHigh())
	}
}

func TestSpanToEventCarriesTheOccurrenceTime(t *testing.T) {
	t.Parallel()

	event := convert(snapshot(tracetest.SpanStub{Name: "x", StartTime: time.Unix(1700000000, 1)}))
	occurrence := event.GetOccurrenceTime()
	if occurrence == nil {
		t.Fatal("SpanToEvent must populate occurrence_time")
	}
	if got := int128.ToBigInt(occurrence.GetNanoseconds()); got.Cmp(big.NewInt(1700000000000000001)) != 0 {
		t.Fatalf("nanoseconds = %s", got)
	}
}

package int128_test

import (
	"math"
	"math/big"
	"testing"
	"time"

	modelv1 "github.com/emet-labs/sentinel-probe-go/gen/sentinel/model/v1"
	"github.com/emet-labs/sentinel-probe-go/int128"
)

func mustBig(t *testing.T, decimal string) *big.Int {
	t.Helper()
	value, ok := new(big.Int).SetString(decimal, 10)
	if !ok {
		t.Fatalf("bad literal %q", decimal)
	}
	return value
}

func TestRoundTrip(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name  string
		value string
	}{
		{"zero", "0"},
		{"one", "1"},
		{"fits in the low word", "1700000000123456789"},
		{"max low word", "18446744073709551615"},
		{"spans both words", "18446744073709551616"}, // 2^64
		{"large positive", "170141183460469231731687303715884105727"},
		{"negative one", "-1"},
		{"negative spanning both words", "-18446744073709551617"},
		{"large negative", "-170141183460469231731687303715884105728"},
	}
	for _, tc := range cases {
		want := mustBig(t, tc.value)
		got := int128.ToBigInt(int128.FromBigInt(want))
		if got.Cmp(want) != 0 {
			t.Errorf("%s: round-trip = %s, want %s", tc.name, got, want)
		}
	}
}

// TestEncodeWordSignedness pins the direction that is easy to get backwards: low is fixed64
// (unsigned) and high is sfixed64 (signed).
func TestEncodeWordSignedness(t *testing.T) {
	t.Parallel()

	positive := int128.FromBigInt(mustBig(t, "18446744073709551617")) // 2^64 + 1
	if positive.GetHigh() != 1 || positive.GetLow() != 1 {
		t.Fatalf("2^64+1 encoded as high=%d low=%d, want high=1 low=1", positive.GetHigh(), positive.GetLow())
	}

	// The worked check from the package doc: -1 is high=-1, low=0xFFFFFFFFFFFFFFFF, and
	// -1*2^64 + (2^64-1) = -1 with NO sign correction on decode.
	negative := int128.FromBigInt(big.NewInt(-1))
	if negative.GetHigh() != -1 {
		t.Fatalf("high = %d, want -1 (arithmetic shift, sign-extended)", negative.GetHigh())
	}
	if negative.GetLow() != math.MaxUint64 {
		t.Fatalf("low = %d, want 2^64-1 (unsigned two's-complement word)", negative.GetLow())
	}
	if decoded := int128.ToBigInt(negative); decoded.Cmp(big.NewInt(-1)) != 0 {
		t.Fatalf("decode applied a second sign correction: got %s, want -1", decoded)
	}
}

func TestFromInt64MatchesFromBigInt(t *testing.T) {
	t.Parallel()

	for _, value := range []int64{0, 1, -1, math.MaxInt64, math.MinInt64, 1700000000123456789, -1700000000123456789} {
		fast := int128.FromInt64(value)
		slow := int128.FromBigInt(big.NewInt(value))
		if fast.GetHigh() != slow.GetHigh() || fast.GetLow() != slow.GetLow() {
			t.Errorf("FromInt64(%d) = {%d,%d}, FromBigInt = {%d,%d}",
				value, fast.GetHigh(), fast.GetLow(), slow.GetHigh(), slow.GetLow())
		}
		if got := int128.ToBigInt(fast); got.Cmp(big.NewInt(value)) != 0 {
			t.Errorf("FromInt64(%d) round-trips to %s", value, got)
		}
	}
}

func TestToBigIntIsNilSafe(t *testing.T) {
	t.Parallel()

	if got := int128.ToBigInt(nil); got.Sign() != 0 {
		t.Fatalf("nil Int128 must decode to zero, got %s", got)
	}
	if got := int128.ToBigInt(&modelv1.Int128{}); got.Sign() != 0 {
		t.Fatalf("zero Int128 must decode to zero, got %s", got)
	}
}

func TestTimeToNanoseconds(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name string
		time time.Time
		want string
	}{
		{"epoch", time.Unix(0, 0), "0"},
		{"whole seconds", time.Unix(1700000000, 0), "1700000000000000000"},
		{"seconds and nanos", time.Unix(1700000000, 123456789), "1700000000123456789"},
		{"before the epoch", time.Unix(-1, 500000000), "-500000000"},
		// time.Time.UnixNano() is documented as undefined outside 1678-2262; the big form is
		// exact, which is the whole reason it is used.
		{"year 3000", time.Date(3000, time.January, 1, 0, 0, 0, 0, time.UTC), "32503680000000000000"},
		{"year 1000", time.Date(1000, time.January, 1, 0, 0, 0, 0, time.UTC), "-30610224000000000000"},
	}
	for _, tc := range cases {
		got := int128.TimeToNanoseconds(tc.time)
		if got.Cmp(mustBig(t, tc.want)) != 0 {
			t.Errorf("%s: TimeToNanoseconds = %s, want %s", tc.name, got, tc.want)
		}
	}
}

func TestTimeToNanosecondsRoundTripsThroughInt128(t *testing.T) {
	t.Parallel()

	for _, moment := range []time.Time{
		time.Unix(1700000000, 123456789),
		time.Date(3000, time.January, 1, 0, 0, 0, 1, time.UTC),
		time.Date(1000, time.January, 1, 0, 0, 0, 0, time.UTC),
	} {
		want := int128.TimeToNanoseconds(moment)
		got := int128.ToBigInt(int128.FromBigInt(want))
		if got.Cmp(want) != 0 {
			t.Errorf("%v: round-trip = %s, want %s", moment, got, want)
		}
	}
}

func TestTimeToNanosecondsBeyondInt64Range(t *testing.T) {
	t.Parallel()

	// The value here overflows int64 nanoseconds (max ~year 2262), so a UnixNano()-based
	// implementation would silently wrap. The Int128 encoding must span both words.
	moment := time.Date(3000, time.January, 1, 0, 0, 0, 0, time.UTC)
	nanoseconds := int128.TimeToNanoseconds(moment)
	if nanoseconds.IsInt64() {
		t.Fatalf("test precondition: %s must not fit in an int64", nanoseconds)
	}
	encoded := int128.FromBigInt(nanoseconds)
	if encoded.GetHigh() == 0 {
		t.Fatalf("a value beyond 2^64 must use the high word, got high=%d", encoded.GetHigh())
	}
	if got := int128.ToBigInt(encoded); got.Cmp(nanoseconds) != 0 {
		t.Fatalf("round-trip = %s, want %s", got, nanoseconds)
	}
}

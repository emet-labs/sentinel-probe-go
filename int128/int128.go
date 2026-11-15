// Package int128 encodes and decodes sentinel.model.v1.Int128, the 128-bit occurrence-time
// representation. Go analog of sdk/typescript/src/util/int128.ts.
//
// Go has no native int128, so math/big.Int is the canonical representation — the idiomatic
// equivalent of TypeScript's bigint, and what makes the round-trip meaningful. FromInt64
// covers the common nanosecond case without allocating a big.Int.
//
// Encode direction is the trap. In common.proto:7-10, `low` is fixed64 (UNSIGNED) and `high`
// is sfixed64 (SIGNED), and the value is high*2^64 + low:
//
//   - encode: low = value mod 2^64 (as unsigned), high = value >> 64 (ARITHMETIC shift);
//   - decode: value = high*2^64 + low, with NO sign correction — high is already signed.
//
// Worked check for value = -1: high = -1, low = 0xFFFFFFFFFFFFFFFF, and
// -1*2^64 + (2^64 - 1) = -1. Applying a second sign correction on decode is a real bug that
// the TypeScript port had to have caught on review; it is not applied here either.
package int128

import (
	"math/big"
	"time"

	modelv1 "github.com/emet-labs/sentinel-probe-go/gen/sentinel/model/v1"
)

// twoPow64 is 2^64, the weight of the high word.
var twoPow64 = new(big.Int).Lsh(big.NewInt(1), 64)

// lowMask masks the low 64 bits. big.Int bitwise operations behave as if operands were in
// infinite two's complement, so And also yields the correct unsigned low word for negatives.
var lowMask = new(big.Int).Sub(twoPow64, big.NewInt(1))

// ToBigInt decodes an Int128 into its integer value: high*2^64 + low. A nil Int128 decodes to
// zero, matching the generated getters' nil-safety.
func ToBigInt(value *modelv1.Int128) *big.Int {
	high := big.NewInt(value.GetHigh()) // sfixed64: already signed
	low := new(big.Int).SetUint64(value.GetLow())
	return high.Mul(high, twoPow64).Add(high, low)
}

// FromBigInt encodes an integer value into an Int128. Values outside the signed 128-bit range
// are truncated to their low 128 bits, which is the same modular behaviour the proto's fixed
// words have.
func FromBigInt(value *big.Int) *modelv1.Int128 {
	low := new(big.Int).And(value, lowMask)
	high := new(big.Int).Rsh(value, 64) // arithmetic on negatives: Rsh is floor division
	return &modelv1.Int128{High: high.Int64(), Low: low.Uint64()}
}

// FromInt64 encodes an int64 without allocating a big.Int. high is the sign extension
// (0 or -1) and low is the two's-complement bit pattern.
func FromInt64(value int64) *modelv1.Int128 {
	return &modelv1.Int128{High: value >> 63, Low: uint64(value)}
}

// TimeToNanoseconds returns t as nanoseconds since the Unix epoch. Analog of
// hrTimeToNanoseconds.
//
// Deliberately NOT t.UnixNano(), which is documented as undefined outside 1678-2262: the
// big form is exact for every representable time and is the literal analog of TypeScript's
// BigInt(seconds)*1e9 + BigInt(nanos). Go's time.Time carries native nanosecond precision, so
// the float-precision hazard that motivated the TypeScript design does not exist here — but
// the Int128 encoding hazard above very much does.
func TimeToNanoseconds(t time.Time) *big.Int {
	seconds := big.NewInt(t.Unix())
	seconds.Mul(seconds, big.NewInt(int64(time.Second/time.Nanosecond)))
	return seconds.Add(seconds, big.NewInt(int64(t.Nanosecond())))
}

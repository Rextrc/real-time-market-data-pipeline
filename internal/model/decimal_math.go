package model

import (
	"math"
	"math/big"
)

// Exact fixed-point arithmetic.
//
// The subtlety here is intermediate overflow. Two scale-8 values — an
// ordinary crypto price and quantity — multiply to a scale-16 result whose
// unscaled integer is around 1e20, well past int64. Saturating at that point
// produces a nonsense number that silently poisons every downstream
// calculation, so the intermediate is computed in arbitrary precision and
// only then reduced to a scale that fits, rounding half away from zero.
//
// Each operation keeps an int64 fast path for the common case, because these
// run per tick.

// Cmp returns -1 if d < o, 0 if equal, +1 if d > o.
func (d Decimal) Cmp(o Decimal) int {
	if d.Scale == o.Scale {
		switch {
		case d.Unscaled < o.Unscaled:
			return -1
		case d.Unscaled > o.Unscaled:
			return 1
		default:
			return 0
		}
	}
	if x, y, ok := alignFast(d, o); ok {
		switch {
		case x < y:
			return -1
		case x > y:
			return 1
		default:
			return 0
		}
	}

	scale := maxScaleOf(d, o)
	return d.bigAt(scale).Cmp(o.bigAt(scale))
}

// Less reports whether d < o.
func (d Decimal) Less(o Decimal) bool { return d.Cmp(o) < 0 }

// Equal reports whether d and o represent the same value regardless of the
// scale each is stored at. 1.50 and 1.5 are equal.
func (d Decimal) Equal(o Decimal) bool { return d.Cmp(o) == 0 }

// Add returns d + o.
func (d Decimal) Add(o Decimal) Decimal {
	if x, y, ok := alignFast(d, o); ok {
		if sum, ok := addFast(x, y); ok {
			return Decimal{Unscaled: sum, Scale: maxScaleOf(d, o)}
		}
	}

	scale := maxScaleOf(d, o)
	return fromBig(new(big.Int).Add(d.bigAt(scale), o.bigAt(scale)), int(scale))
}

// Sub returns d - o.
func (d Decimal) Sub(o Decimal) Decimal { return d.Add(o.Neg()) }

// Mul returns d * o. The exact result has the sum of the operands' scales;
// if that does not fit, the scale is reduced with rounding rather than the
// value being clamped.
func (d Decimal) Mul(o Decimal) Decimal {
	scale := int(d.Scale) + int(o.Scale)
	if scale <= maxScale && !overflowsMul(d.Unscaled, o.Unscaled) {
		return Decimal{Unscaled: d.Unscaled * o.Unscaled, Scale: uint8(scale)}
	}
	return fromBig(new(big.Int).Mul(big.NewInt(d.Unscaled), big.NewInt(o.Unscaled)), scale)
}

// Div returns d / o at the given scale, rounding half away from zero.
// Dividing by zero returns a zero Decimal — callers must check first.
func (d Decimal) Div(o Decimal, scale uint8) Decimal {
	if o.Unscaled == 0 {
		return Decimal{}
	}
	if scale > maxScale {
		scale = maxScale
	}

	// (d.Unscaled * 10^(scale + o.Scale - d.Scale)) / o.Unscaled
	shift := int(scale) + int(o.Scale) - int(d.Scale)
	num := big.NewInt(d.Unscaled)
	den := big.NewInt(o.Unscaled)
	if shift >= 0 {
		num.Mul(num, pow10Big(shift))
	} else {
		den.Mul(den, pow10Big(-shift))
	}

	q, r := new(big.Int).QuoRem(num, den, new(big.Int))
	// Round half away from zero.
	r.Abs(r).Lsh(r, 1)
	if r.Cmp(new(big.Int).Abs(den)) >= 0 {
		if (d.Unscaled < 0) != (o.Unscaled < 0) {
			q.Sub(q, big.NewInt(1))
		} else {
			q.Add(q, big.NewInt(1))
		}
	}
	return fromBig(q, int(scale))
}

// Neg returns -d.
func (d Decimal) Neg() Decimal { return Decimal{Unscaled: -d.Unscaled, Scale: d.Scale} }

// IsNegative reports whether d < 0.
func (d Decimal) IsNegative() bool { return d.Unscaled < 0 }

// Float converts to float64. Only for display, ratios, and indicator math —
// never for money that will be stored or compared. Named explicitly so every
// lossy conversion is visible at the call site.
func (d Decimal) Float() float64 {
	return float64(d.Unscaled) / math.Pow10(int(d.Scale))
}

// FromFloat converts a float to a Decimal at the given scale, rounding half
// away from zero. Use it for derived quantities such as a computed position
// size, never for a price that arrived as text.
func FromFloat(f float64, scale uint8) Decimal {
	if scale > maxScale {
		scale = maxScale
	}
	scaled := f * math.Pow10(int(scale))
	if math.IsNaN(scaled) || math.IsInf(scaled, 0) ||
		scaled > math.MaxInt64 || scaled < math.MinInt64 {
		return saturate(scaled)
	}
	return Decimal{Unscaled: int64(math.Round(scaled)), Scale: scale}
}

// Rescale returns d expressed at the given scale. Reducing the scale rounds
// half away from zero and therefore loses information.
func (d Decimal) Rescale(scale uint8) Decimal {
	switch {
	case scale == d.Scale:
		return d
	case scale > d.Scale:
		factor := pow10(scale - d.Scale)
		if !overflowsMul(d.Unscaled, factor) {
			return Decimal{Unscaled: d.Unscaled * factor, Scale: scale}
		}
		return d // cannot widen without overflow; the value is unchanged
	default:
		factor := pow10(d.Scale - scale)
		q, r := d.Unscaled/factor, d.Unscaled%factor
		if r*2 >= factor {
			q++
		} else if r*2 <= -factor {
			q--
		}
		return Decimal{Unscaled: q, Scale: scale}
	}
}

// bigAt renders d's value as an unscaled big.Int at the given scale.
func (d Decimal) bigAt(scale uint8) *big.Int {
	v := big.NewInt(d.Unscaled)
	if scale > d.Scale {
		v.Mul(v, pow10Big(int(scale-d.Scale)))
	}
	return v
}

// fromBig reduces an arbitrary-precision unscaled value to a Decimal,
// dropping scale (with rounding) until the mantissa fits in an int64.
func fromBig(v *big.Int, scale int) Decimal {
	if scale > maxScale {
		v = reduceScale(v, scale-maxScale)
		scale = maxScale
	}
	for scale > 0 && !v.IsInt64() {
		v = reduceScale(v, 1)
		scale--
	}
	if !v.IsInt64() {
		if v.Sign() >= 0 {
			return Decimal{Unscaled: math.MaxInt64, Scale: 0}
		}
		return Decimal{Unscaled: math.MinInt64 + 1, Scale: 0}
	}
	return Decimal{Unscaled: v.Int64(), Scale: uint8(scale)}
}

// reduceScale divides by 10^n, rounding half away from zero.
func reduceScale(v *big.Int, n int) *big.Int {
	den := pow10Big(n)
	q, r := new(big.Int).QuoRem(v, den, new(big.Int))

	r.Abs(r).Lsh(r, 1)
	if r.Cmp(den) >= 0 {
		if v.Sign() < 0 {
			q.Sub(q, big.NewInt(1))
		} else {
			q.Add(q, big.NewInt(1))
		}
	}
	return q
}

func pow10Big(n int) *big.Int {
	return new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(n)), nil)
}

func maxScaleOf(a, b Decimal) uint8 {
	if a.Scale > b.Scale {
		return a.Scale
	}
	return b.Scale
}

// alignFast rescales two decimals to a common scale in int64, reporting
// whether it could be done without overflow.
func alignFast(a, b Decimal) (int64, int64, bool) {
	switch {
	case a.Scale == b.Scale:
		return a.Unscaled, b.Unscaled, true
	case a.Scale < b.Scale:
		f := pow10(b.Scale - a.Scale)
		if overflowsMul(a.Unscaled, f) {
			return 0, 0, false
		}
		return a.Unscaled * f, b.Unscaled, true
	default:
		f := pow10(a.Scale - b.Scale)
		if overflowsMul(b.Unscaled, f) {
			return 0, 0, false
		}
		return a.Unscaled, b.Unscaled * f, true
	}
}

func addFast(x, y int64) (int64, bool) {
	sum := x + y
	// Two's-complement overflow shows as a sign the operands cannot produce.
	if (x > 0 && y > 0 && sum < 0) || (x < 0 && y < 0 && sum >= 0) {
		return 0, false
	}
	return sum, true
}

func overflowsMul(a, b int64) bool {
	if a == 0 || b == 0 {
		return false
	}
	p := a * b
	return p/b != a
}

func pow10(n uint8) int64 {
	if n > maxScale {
		return math.MaxInt64
	}
	p := int64(1)
	for i := uint8(0); i < n; i++ {
		p *= 10
	}
	return p
}

func saturate(f float64) Decimal {
	switch {
	case math.IsNaN(f):
		return Decimal{}
	case f > 0:
		return Decimal{Unscaled: math.MaxInt64, Scale: 0}
	default:
		return Decimal{Unscaled: math.MinInt64 + 1, Scale: 0}
	}
}

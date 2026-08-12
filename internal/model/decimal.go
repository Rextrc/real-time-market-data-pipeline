package model

import (
	"errors"
	"strconv"
	"strings"
)

// Decimal is an exact base-10 fixed-point number, stored as an unscaled
// integer plus a scale (number of digits after the point). 1.2345 is
// {Unscaled: 12345, Scale: 4}.
//
// Prices and quantities are never float64. Binary floating point cannot
// represent most decimal fractions exactly, so the error is silent, tiny,
// and permanently baked into every row written before it is noticed.
type Decimal struct {
	Unscaled int64
	Scale    uint8
}

const maxScale = 18

var (
	ErrEmptyDecimal   = errors.New("model: empty decimal")
	ErrDecimalSyntax  = errors.New("model: malformed decimal")
	ErrDecimalRange   = errors.New("model: decimal out of range")
	errDecimalTooFine = errors.New("model: decimal has more than 18 fractional digits")
)

// ParseDecimal converts an exchange's string-encoded number ("0.00123400")
// into a Decimal without going through a float at any point.
func ParseDecimal(s string) (Decimal, error) {
	if s == "" {
		return Decimal{}, ErrEmptyDecimal
	}

	neg := false
	switch s[0] {
	case '-':
		neg, s = true, s[1:]
	case '+':
		s = s[1:]
	}
	if s == "" {
		return Decimal{}, ErrDecimalSyntax
	}

	intPart, fracPart, hasPoint := strings.Cut(s, ".")
	if hasPoint && strings.Contains(fracPart, ".") {
		return Decimal{}, ErrDecimalSyntax
	}
	if intPart == "" && fracPart == "" {
		return Decimal{}, ErrDecimalSyntax
	}
	if len(fracPart) > maxScale {
		return Decimal{}, errDecimalTooFine
	}

	digits := intPart + fracPart
	if digits == "" {
		return Decimal{}, ErrDecimalSyntax
	}
	for i := 0; i < len(digits); i++ {
		if digits[i] < '0' || digits[i] > '9' {
			return Decimal{}, ErrDecimalSyntax
		}
	}

	unscaled, err := strconv.ParseInt(digits, 10, 64)
	if err != nil {
		return Decimal{}, ErrDecimalRange
	}
	if neg {
		unscaled = -unscaled
	}
	return Decimal{Unscaled: unscaled, Scale: uint8(len(fracPart))}, nil
}

// String renders the exact value. It round-trips through ParseDecimal.
func (d Decimal) String() string {
	if d.Scale == 0 {
		return strconv.FormatInt(d.Unscaled, 10)
	}

	neg := d.Unscaled < 0
	mag := d.Unscaled
	if neg {
		mag = -mag // int64 min is not reachable via ParseDecimal
	}

	digits := strconv.FormatInt(mag, 10)
	if len(digits) <= int(d.Scale) {
		digits = strings.Repeat("0", int(d.Scale)-len(digits)+1) + digits
	}
	split := len(digits) - int(d.Scale)

	var b strings.Builder
	if neg {
		b.WriteByte('-')
	}
	b.WriteString(digits[:split])
	b.WriteByte('.')
	b.WriteString(digits[split:])
	return b.String()
}

// IsZero reports whether the value is exactly zero at any scale.
func (d Decimal) IsZero() bool { return d.Unscaled == 0 }

// MarshalJSON emits the value as a JSON string, not a number.
//
// A JSON number would be parsed as a float64 by almost every client,
// reintroducing at the API boundary exactly the precision loss this type
// exists to prevent. Every serious financial API quotes prices as strings
// for this reason.
func (d Decimal) MarshalJSON() ([]byte, error) {
	s := d.String()
	out := make([]byte, 0, len(s)+2)
	out = append(out, '"')
	out = append(out, s...)
	return append(out, '"'), nil
}

// UnmarshalJSON accepts a quoted decimal string, and also a bare JSON number
// so that hand-written payloads still work.
func (d *Decimal) UnmarshalJSON(b []byte) error {
	s := strings.TrimSpace(string(b))
	if s == "null" {
		*d = Decimal{}
		return nil
	}
	s = strings.Trim(s, `"`)

	parsed, err := ParseDecimal(s)
	if err != nil {
		return err
	}
	*d = parsed
	return nil
}

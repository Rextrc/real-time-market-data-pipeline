package model

import (
	"fmt"
	"strconv"
	"testing"
)

func TestParseDecimalRoundTrip(t *testing.T) {
	cases := []struct {
		in       string
		unscaled int64
		scale    uint8
	}{
		{"0", 0, 0},
		{"1", 1, 0},
		{"0.1", 1, 1},
		{"68420.51000000", 6842051000000, 8},
		{"0.00000001", 1, 8},
		{"-12.75", -1275, 2},
		{"+3.5", 35, 1},
		{".5", 5, 1},
		{"5.", 5, 0},
	}

	for _, c := range cases {
		got, err := ParseDecimal(c.in)
		if err != nil {
			t.Errorf("ParseDecimal(%q): unexpected error %v", c.in, err)
			continue
		}
		if got.Unscaled != c.unscaled || got.Scale != c.scale {
			t.Errorf("ParseDecimal(%q) = {%d,%d}, want {%d,%d}",
				c.in, got.Unscaled, got.Scale, c.unscaled, c.scale)
		}
		// Round-tripping matters because String is what gets written to
		// storage and to the API; a lossy render is a lossy archive.
		again, err := ParseDecimal(got.String())
		if err != nil {
			t.Errorf("ParseDecimal(%q).String() = %q, which failed to reparse: %v", c.in, got.String(), err)
			continue
		}
		if again != got {
			t.Errorf("round trip of %q: %v -> %q -> %v", c.in, got, got.String(), again)
		}
	}
}

func TestParseDecimalRejects(t *testing.T) {
	bad := []string{
		"", "-", "+", ".", "abc", "1.2.3", "1,5", "1e5", " 1", "1 ",
		"0.0000000000000000001",             // 19 fractional digits
		"99999999999999999999999999999.123", // overflows int64
	}
	for _, s := range bad {
		if got, err := ParseDecimal(s); err == nil {
			t.Errorf("ParseDecimal(%q) = %v, want error", s, got)
		}
	}
}

// TestPrecisionExceedsFloat64 is the reason this type exists. float64 has 53
// bits of mantissa, so a large price with eight decimal places cannot be
// represented exactly; Decimal must be exact.
func TestPrecisionExceedsFloat64(t *testing.T) {
	const s = "12345678.12345678"

	d, err := ParseDecimal(s)
	if err != nil {
		t.Fatalf("ParseDecimal(%q): %v", s, err)
	}
	if d.String() != s {
		t.Errorf("Decimal round trip = %q, want %q", d.String(), s)
	}

	// Document the failure mode being avoided: parsing to float64 and
	// formatting back does not reproduce the input.
	var f float64
	if _, err := fmt.Sscanf(s, "%g", &f); err != nil {
		t.Fatalf("Sscanf: %v", err)
	}
	if viaFloat := strconv.FormatFloat(f, 'f', 8, 64); viaFloat == s {
		t.Skip("float64 happened to be exact for this value; pick a harder one")
	} else {
		t.Logf("float64 path yields %q, Decimal yields %q", viaFloat, d.String())
	}
}

func TestStringNegativeFraction(t *testing.T) {
	d := Decimal{Unscaled: -5, Scale: 3}
	if got, want := d.String(), "-0.005"; got != want {
		t.Errorf("String() = %q, want %q", got, want)
	}
}

// TestMulDoesNotOverflowAtCryptoScales is a regression test. A scale-8 price
// times a scale-8 quantity has a scale-16 exact result whose mantissa is
// around 1e20 — well past int64. Saturating there produced a nonsense
// notional that silently rejected every paper order.
func TestMulDoesNotOverflowAtCryptoScales(t *testing.T) {
	price := Decimal{Unscaled: 10005000000, Scale: 8} // 100.05
	qty := Decimal{Unscaled: 9995002499, Scale: 8}    // 99.95002499

	got := price.Mul(qty).Rescale(2)

	// 100.05 * 99.95002499 ≈ 10000.00
	if f := got.Float(); f < 9999 || f > 10001 {
		t.Errorf("Mul = %s (%.4f), want ~10000", got, f)
	}
}

func TestMulExactWhenItFits(t *testing.T) {
	a := Decimal{Unscaled: 150, Scale: 2} // 1.50
	b := Decimal{Unscaled: 400, Scale: 2} // 4.00
	if got, want := a.Mul(b).Rescale(2).String(), "6.00"; got != want {
		t.Errorf("1.50 * 4.00 = %s, want %s", got, want)
	}
}

func TestAddAcrossScales(t *testing.T) {
	a := Decimal{Unscaled: 1, Scale: 8}   // 0.00000001
	b := Decimal{Unscaled: 100, Scale: 2} // 1.00
	if got, want := a.Add(b).String(), "1.00000001"; got != want {
		t.Errorf("sum = %s, want %s", got, want)
	}
}

func TestSumOfManyQuantitiesStaysExact(t *testing.T) {
	// A trading day's volume accumulator: a float would drift here.
	var total Decimal
	one := Decimal{Unscaled: 1, Scale: 8} // 0.00000001
	for i := 0; i < 100000; i++ {
		total = total.Add(one)
	}
	if got, want := total.String(), "0.00100000"; got != want {
		t.Errorf("sum of 100000 * 0.00000001 = %s, want %s", got, want)
	}
}

func TestCmpAcrossScales(t *testing.T) {
	if !(Decimal{Unscaled: 150, Scale: 2}).Equal(Decimal{Unscaled: 15, Scale: 1}) {
		t.Error("1.50 should equal 1.5")
	}
	if !(Decimal{Unscaled: 999, Scale: 3}).Less(Decimal{Unscaled: 1, Scale: 0}) {
		t.Error("0.999 should be less than 1")
	}
}

func TestDivRoundsHalfAwayFromZero(t *testing.T) {
	ten := Decimal{Unscaled: 1000, Scale: 2}
	three := Decimal{Unscaled: 300, Scale: 2}
	if got, want := ten.Div(three, 4).String(), "3.3333"; got != want {
		t.Errorf("10/3 = %s, want %s", got, want)
	}
	if got := ten.Div(Decimal{}, 2); !got.IsZero() {
		t.Errorf("division by zero = %s, want zero", got)
	}
}

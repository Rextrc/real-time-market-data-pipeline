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

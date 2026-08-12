package normalize

import (
	"errors"
	"testing"
	"time"

	"github.com/Rextrc/real-time-market-data-pipeline/internal/instrument"
	"github.com/Rextrc/real-time-market-data-pipeline/internal/model"
	"github.com/Rextrc/real-time-market-data-pipeline/internal/venue"
)

const tradeFrame = `{"stream":"btcusdt@trade","data":{` +
	`"e":"trade","E":1710000000123,"s":"BTCUSDT","t":3456789,` +
	`"p":"68420.51000000","q":"0.00312000","T":1710000000100,"m":true,"M":true}}`

func testRegistry(t *testing.T) *instrument.Registry {
	t.Helper()
	reg := instrument.NewRegistry()
	err := reg.Register(instrument.Mapping{
		Venue: model.VenueBinance, Instrument: "BTC-USDT", Symbol: "BTCUSDT",
	})
	if err != nil {
		t.Fatalf("register: %v", err)
	}
	return reg
}

func frame(payload string) venue.RawMessage {
	return venue.RawMessage{
		Venue:    model.VenueBinance,
		RecvTime: time.UnixMilli(1710000000200).UTC(),
		Payload:  []byte(payload),
	}
}

func TestBinanceDecodeTrade(t *testing.T) {
	d := NewBinanceDecoder(testRegistry(t))

	ticks, err := d.Decode(frame(tradeFrame))
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}
	if len(ticks) != 1 {
		t.Fatalf("got %d ticks, want 1", len(ticks))
	}
	got := ticks[0]

	if got.Instrument != "BTC-USDT" {
		t.Errorf("Instrument = %q, want BTC-USDT", got.Instrument)
	}
	if got.Price.String() != "68420.51000000" {
		t.Errorf("Price = %q, want 68420.51000000", got.Price)
	}
	if got.Quantity.String() != "0.00312000" {
		t.Errorf("Quantity = %q, want 0.00312000", got.Quantity)
	}
	// m=true means the buyer was the maker, so the seller was the aggressor.
	if got.Side != model.SideSell {
		t.Errorf("Side = %v, want sell", got.Side)
	}
	if got.VenueTradeID != "3456789" {
		t.Errorf("VenueTradeID = %q, want 3456789", got.VenueTradeID)
	}
	// EventTime must come from T (match time), not E (emit time).
	if want := time.UnixMilli(1710000000100).UTC(); !got.EventTime.Equal(want) {
		t.Errorf("EventTime = %v, want %v", got.EventTime, want)
	}
	if want := 100 * time.Millisecond; got.Lag() != want {
		t.Errorf("Lag() = %v, want %v", got.Lag(), want)
	}
}

func TestBinanceDecodeSideFromMakerFlag(t *testing.T) {
	d := NewBinanceDecoder(testRegistry(t))
	payload := `{"stream":"btcusdt@trade","data":{"e":"trade","E":1,"s":"BTCUSDT",` +
		`"t":1,"p":"1.0","q":"1.0","T":1,"m":false}}`

	ticks, err := d.Decode(frame(payload))
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}
	if ticks[0].Side != model.SideBuy {
		t.Errorf("Side = %v, want buy when m=false", ticks[0].Side)
	}
}

// TestBinanceDecodeSideIsNotFlippedByIgnoreFlag is a regression test.
// Binance sends both "m" (buyer-is-maker) and "M" (a deprecated ignore
// flag). encoding/json falls back to case-insensitive key matching, so if
// "M" is not bound to a field of its own it binds to "m" instead and
// inverts the side of every trade — a bug that is invisible in aggregate
// and corrupts every buy/sell ratio computed from the archive.
func TestBinanceDecodeSideIsNotFlippedByIgnoreFlag(t *testing.T) {
	d := NewBinanceDecoder(testRegistry(t))

	for _, tc := range []struct {
		payload string
		want    model.Side
	}{
		{`{"stream":"x","data":{"e":"trade","s":"BTCUSDT","t":1,"p":"1","q":"1","T":1,"m":false,"M":true}}`, model.SideBuy},
		{`{"stream":"x","data":{"e":"trade","s":"BTCUSDT","t":1,"p":"1","q":"1","T":1,"m":true,"M":true}}`, model.SideSell},
		{`{"stream":"x","data":{"e":"trade","s":"BTCUSDT","t":1,"p":"1","q":"1","T":1,"M":true,"m":false}}`, model.SideBuy},
	} {
		ticks, err := d.Decode(frame(tc.payload))
		if err != nil {
			t.Fatalf("Decode: %v", err)
		}
		if ticks[0].Side != tc.want {
			t.Errorf("Side = %v, want %v for %s", ticks[0].Side, tc.want, tc.payload)
		}
	}
}

func TestBinanceDecodeIgnoresControlFrames(t *testing.T) {
	d := NewBinanceDecoder(testRegistry(t))

	for _, payload := range []string{
		`{"result":null,"id":1}`,
		`{"stream":"btcusdt@depth","data":{"e":"depthUpdate","s":"BTCUSDT"}}`,
	} {
		ticks, err := d.Decode(frame(payload))
		if err != nil {
			t.Errorf("Decode(%s): unexpected error %v", payload, err)
		}
		if len(ticks) != 0 {
			t.Errorf("Decode(%s): got %d ticks, want 0", payload, len(ticks))
		}
	}
}

// TestBinanceDecodeRejectsMalformed is the M2 break-it exercise as a test:
// a bad frame must produce a loud, specific error, never a zero-valued tick
// that flows silently into storage.
func TestBinanceDecodeRejectsMalformed(t *testing.T) {
	d := NewBinanceDecoder(testRegistry(t))

	cases := map[string]string{
		"not json":                               `{`,
		"price is a number rather than a string": `{"stream":"x","data":{"e":"trade","s":"BTCUSDT","t":1,"p":68420.51,"q":"1","T":1}}`,
		"empty price":                            `{"stream":"x","data":{"e":"trade","s":"BTCUSDT","t":1,"p":"","q":"1","T":1}}`,
		"missing symbol":                         `{"stream":"x","data":{"e":"trade","t":1,"p":"1","q":"1","T":1}}`,
		"unmapped symbol":                        `{"stream":"x","data":{"e":"trade","s":"DOGEUSDT","t":1,"p":"1","q":"1","T":1}}`,
	}

	for name, payload := range cases {
		ticks, err := d.Decode(frame(payload))
		if err == nil {
			t.Errorf("%s: got %d ticks and no error, want error", name, len(ticks))
		}
	}
}

func TestNormalizerAssignsPerInstrumentSequence(t *testing.T) {
	reg := testRegistry(t)
	if err := reg.Register(instrument.Mapping{
		Venue: model.VenueBinance, Instrument: "ETH-USDT", Symbol: "ETHUSDT",
	}); err != nil {
		t.Fatalf("register: %v", err)
	}
	n := New(NewBinanceDecoder(reg))

	mk := func(sym string) string {
		return `{"stream":"x","data":{"e":"trade","E":1,"s":"` + sym +
			`","t":1,"p":"1.0","q":"1.0","T":1,"m":false}}`
	}

	want := []struct {
		sym string
		seq model.Seq
	}{
		{"BTCUSDT", 1}, {"BTCUSDT", 2}, {"ETHUSDT", 1}, {"BTCUSDT", 3}, {"ETHUSDT", 2},
	}
	for i, w := range want {
		ticks, err := n.Normalize(frame(mk(w.sym)))
		if err != nil {
			t.Fatalf("step %d: %v", i, err)
		}
		if ticks[0].Seq != w.seq {
			t.Errorf("step %d (%s): Seq = %d, want %d", i, w.sym, ticks[0].Seq, w.seq)
		}
	}
}

func TestNormalizerReportsIgnored(t *testing.T) {
	n := New(NewBinanceDecoder(testRegistry(t)))

	_, err := n.Normalize(frame(`{"result":null,"id":1}`))
	if !errors.Is(err, ErrIgnored) {
		t.Errorf("err = %v, want ErrIgnored", err)
	}
}

func TestNormalizerUnknownVenue(t *testing.T) {
	n := New(NewBinanceDecoder(testRegistry(t)))

	msg := frame(tradeFrame)
	msg.Venue = model.VenueCoinbase
	if _, err := n.Normalize(msg); err == nil {
		t.Error("expected an error for a venue with no registered decoder")
	}
}

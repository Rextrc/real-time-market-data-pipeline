package candles

import (
	"testing"
	"time"

	"github.com/Rextrc/real-time-market-data-pipeline/internal/model"
)

func tick(sec int, price string, qty string) model.Tick {
	p, err := model.ParseDecimal(price)
	if err != nil {
		panic(err)
	}
	q, err := model.ParseDecimal(qty)
	if err != nil {
		panic(err)
	}
	return model.Tick{
		Venue:      model.VenueBinance,
		Instrument: "BTC-USDT",
		EventTime:  time.Unix(int64(sec), 0).UTC(),
		Price:      p,
		Quantity:   q,
	}
}

func TestOHLCVAggregation(t *testing.T) {
	b := New(time.Minute, 10)

	// All within the same minute bucket.
	b.Add(tick(0, "100.00", "1.0"))
	b.Add(tick(10, "105.00", "2.0"))
	b.Add(tick(20, "95.00", "0.5"))
	b.Add(tick(30, "102.00", "1.5"))

	c, ok := b.Current(model.VenueBinance, "BTC-USDT")
	if !ok {
		t.Fatal("no open candle")
	}

	for _, tc := range []struct{ name, got, want string }{
		{"open", c.Open.String(), "100.00"},
		{"high", c.High.String(), "105.00"},
		{"low", c.Low.String(), "95.00"},
		{"close", c.Close.String(), "102.00"},
	} {
		if tc.got != tc.want {
			t.Errorf("%s = %s, want %s", tc.name, tc.got, tc.want)
		}
	}
	if got := c.Volume.Float(); got != 5.0 {
		t.Errorf("volume = %v, want 5.0", got)
	}
	if c.Trades != 4 {
		t.Errorf("trades = %d, want 4", c.Trades)
	}
	if c.Complete {
		t.Error("the in-progress bar must not be marked complete")
	}
}

func TestBarRollsOverAtBucketBoundary(t *testing.T) {
	b := New(time.Minute, 10)

	b.Add(tick(30, "100.00", "1.0"))  // minute 0
	b.Add(tick(90, "200.00", "1.0"))  // minute 1
	b.Add(tick(150, "300.00", "1.0")) // minute 2

	bars := b.Recent(model.VenueBinance, "BTC-USDT", 10)
	if len(bars) != 3 {
		t.Fatalf("got %d bars, want 3 (2 closed + 1 open)", len(bars))
	}
	if !bars[0].Complete || !bars[1].Complete {
		t.Error("closed bars must be marked complete")
	}
	if bars[2].Complete {
		t.Error("the final bar is still open")
	}
	if bars[0].Close.String() != "100.00" || bars[1].Close.String() != "200.00" {
		t.Errorf("closes = %s, %s", bars[0].Close, bars[1].Close)
	}
}

// TestLateTickIsDropped documents a deliberate choice: mutating an already
// published bar would make the same query return different answers over time.
func TestLateTickIsDropped(t *testing.T) {
	b := New(time.Minute, 10)

	b.Add(tick(30, "100.00", "1.0"))
	b.Add(tick(90, "200.00", "1.0")) // rolls over
	b.Add(tick(35, "999.00", "1.0")) // late, belongs to the closed bar

	bars := b.Recent(model.VenueBinance, "BTC-USDT", 10)
	for _, bar := range bars {
		if bar.High.String() == "999.00" {
			t.Error("a late tick mutated an already-closed bar")
		}
	}
}

func TestHistoryIsBounded(t *testing.T) {
	const keep = 5
	b := New(time.Minute, keep)

	for i := 0; i < 50; i++ {
		b.Add(tick(i*60+1, "100.00", "1.0"))
	}

	bars := b.Recent(model.VenueBinance, "BTC-USDT", 1000)
	if len(bars) > keep+1 { // +1 for the open bar
		t.Errorf("retained %d bars, want at most %d", len(bars), keep+1)
	}
}

func TestVolumeSumIsExactAcrossScales(t *testing.T) {
	b := New(time.Minute, 10)

	b.Add(tick(1, "100.00", "0.00000001"))
	b.Add(tick(2, "100.00", "1.5"))
	b.Add(tick(3, "100.00", "0.00000002"))

	c, _ := b.Current(model.VenueBinance, "BTC-USDT")
	if got, want := c.Volume.String(), "1.50000003"; got != want {
		t.Errorf("volume = %s, want %s (exact, not float-accumulated)", got, want)
	}
}

func TestSeparateInstrumentsDoNotMix(t *testing.T) {
	b := New(time.Minute, 10)

	btc := tick(1, "100.00", "1.0")
	eth := tick(1, "50.00", "1.0")
	eth.Instrument = "ETH-USDT"

	b.Add(btc)
	b.Add(eth)

	c1, _ := b.Current(model.VenueBinance, "BTC-USDT")
	c2, _ := b.Current(model.VenueBinance, "ETH-USDT")
	if c1.Close.String() != "100.00" || c2.Close.String() != "50.00" {
		t.Errorf("instrument bars mixed: BTC=%s ETH=%s", c1.Close, c2.Close)
	}
}

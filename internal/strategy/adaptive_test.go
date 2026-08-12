package strategy

import (
	"context"
	"testing"

	"github.com/Rextrc/real-time-market-data-pipeline/internal/consumer/candles"
	"github.com/Rextrc/real-time-market-data-pipeline/internal/model"
)

// fakeMarket is the minimal Market a strategy test needs: one instrument
// with a caller-supplied bar series, no ticks.
type fakeMarket struct {
	id   model.InstrumentID
	bars []candles.Candle
}

func (f fakeMarket) Last(model.InstrumentID) (model.Tick, bool) { return model.Tick{}, false }
func (f fakeMarket) Candles(id model.InstrumentID, limit int) []candles.Candle {
	if id != f.id {
		return nil
	}
	if limit > 0 && limit < len(f.bars) {
		return f.bars[len(f.bars)-limit:]
	}
	return f.bars
}
func (f fakeMarket) Instruments() []model.InstrumentID { return []model.InstrumentID{f.id} }

func closeBar(price float64) candles.Candle {
	d := model.FromFloat(price, 2)
	return candles.Candle{Open: d, High: d, Low: d, Close: d, Complete: true}
}

func TestAdaptive_NoSignalOnInsufficientHistory(t *testing.T) {
	a := NewAdaptive([]AdaptiveArm{{2, 4}}, 0, 0)
	mk := fakeMarket{id: "BTC-USDT", bars: []candles.Candle{closeBar(100), closeBar(101)}}

	sigs, err := a.Evaluate(context.Background(), mk)
	if err != nil {
		t.Fatalf("Evaluate: %v", err)
	}
	if len(sigs) != 0 {
		t.Fatalf("expected no signals with too little history, got %d", len(sigs))
	}
}

func TestAdaptive_EntersAndExitsOnCrossover(t *testing.T) {
	// One arm only, epsilon 0, so the outcome is deterministic: whatever
	// crosses is what trades.
	a := NewAdaptive([]AdaptiveArm{{2, 3}}, 0, 0.5)

	// Flat, then a jump on the last closed bar — fast(2) crosses above
	// slow(3) exactly at that bar, not before. A trailing in-progress bar is
	// appended and must be excluded by Evaluate.
	entryPrices := []float64{100, 100, 100, 100, 105, 999}
	mk := fakeMarket{id: "BTC-USDT", bars: barsFrom(entryPrices)}

	sigs, err := a.Evaluate(context.Background(), mk)
	if err != nil {
		t.Fatalf("Evaluate: %v", err)
	}
	if len(sigs) != 1 || sigs[0].Action != ActionBuy {
		t.Fatalf("expected exactly one buy signal on the jump, got %+v", sigs)
	}

	a.mu.Lock()
	_, holding := a.open["BTC-USDT"]
	a.mu.Unlock()
	if !holding {
		t.Fatal("expected an open position to be tracked after entry")
	}

	// Mirror image: flat high, then a drop on the last closed bar — fast(2)
	// crosses below slow(3) exactly there. Should exit and score the trade.
	exitPrices := []float64{110, 110, 110, 110, 90, 999}
	mk.bars = barsFrom(exitPrices)

	sigs, err = a.Evaluate(context.Background(), mk)
	if err != nil {
		t.Fatalf("Evaluate: %v", err)
	}
	if len(sigs) != 1 || sigs[0].Action != ActionSell {
		t.Fatalf("expected exactly one sell signal on the drop, got %+v", sigs)
	}

	a.mu.Lock()
	_, stillHolding := a.open["BTC-USDT"]
	trades := a.trades[0]
	a.mu.Unlock()
	if stillHolding {
		t.Fatal("position should be closed after the exit signal")
	}
	if trades != 1 {
		t.Fatalf("expected 1 recorded trade for the arm, got %d", trades)
	}
}

func barsFrom(prices []float64) []candles.Candle {
	bars := make([]candles.Candle, len(prices))
	for i, p := range prices {
		bars[i] = closeBar(p)
	}
	return bars
}

func TestAdaptive_DefaultArmsWhenNil(t *testing.T) {
	a := NewAdaptive(nil, 0, 0)
	if len(a.Arms) == 0 {
		t.Fatal("expected default arms to be populated")
	}
	if a.Epsilon <= 0 || a.Epsilon >= 1 {
		t.Fatalf("expected a sane default epsilon, got %v", a.Epsilon)
	}
}

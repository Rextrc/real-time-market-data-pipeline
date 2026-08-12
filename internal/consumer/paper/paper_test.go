package paper

import (
	"io"
	"log/slog"
	"math"
	"testing"
	"time"

	"github.com/Rextrc/real-time-market-data-pipeline/internal/model"
	"github.com/Rextrc/real-time-market-data-pipeline/internal/strategy"
)

func quietLog() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

func testConfig() Config {
	return Config{
		StartingCash:        model.FromFloat(10000, 2),
		FeeRate:             0.001,  // 10 bps
		SlippageRate:        0.0005, // 5 bps
		MaxPositionFraction: 1.0,    // full equity, so the arithmetic is easy to check
		PricePrecision:      8,
		QuantityPrecision:   8,
	}
}

func price(s string) model.Decimal {
	d, err := model.ParseDecimal(s)
	if err != nil {
		panic(err)
	}
	return d
}

func buy(id model.InstrumentID) strategy.Signal {
	return strategy.Signal{Instrument: id, Action: strategy.ActionBuy, Strength: 1, Reason: "test"}
}

func sell(id model.InstrumentID) strategy.Signal {
	return strategy.Signal{Instrument: id, Action: strategy.ActionSell, Strength: 1, Reason: "test"}
}

func TestRoundTripAtFlatPriceLosesFeesAndSpread(t *testing.T) {
	a := NewAccount(testConfig(), quietLog())
	now := time.Now().UTC()

	if _, err := a.Apply(buy("BTC-USDT"), price("100.00"), now); err != nil {
		t.Fatalf("buy: %v", err)
	}
	if _, err := a.Apply(sell("BTC-USDT"), price("100.00"), now.Add(time.Minute)); err != nil {
		t.Fatalf("sell: %v", err)
	}

	snap := a.Snapshot(now)

	// Buying and selling at the same headline price must lose money: two
	// fees plus two spread crossings. A paper engine that shows break-even
	// here is lying, and every strategy built on it will look profitable.
	if snap.TotalPL.Float() >= 0 {
		t.Errorf("round trip at a flat price produced P&L of %s; costs are not being applied",
			snap.TotalPL)
	}

	// ~10bps fee twice + ~5bps slippage twice ≈ 30bps of notional.
	loss := -snap.TotalPL.Float()
	if want, tol := 30.0, 6.0; math.Abs(loss-want) > tol {
		t.Errorf("round-trip cost = %.2f, want roughly %.2f (fees %.0fbps + slippage %.0fbps)",
			loss, want, testConfig().FeeRate*1e4, testConfig().SlippageRate*1e4)
	}
	if snap.FeesPaid.Float() <= 0 {
		t.Error("FeesPaid should be positive after a round trip")
	}
	if snap.Losses != 1 || snap.Wins != 0 {
		t.Errorf("wins/losses = %d/%d, want 0/1", snap.Wins, snap.Losses)
	}
}

func TestProfitableMoveClearsCosts(t *testing.T) {
	a := NewAccount(testConfig(), quietLog())
	now := time.Now().UTC()

	_, _ = a.Apply(buy("BTC-USDT"), price("100.00"), now)
	_, _ = a.Apply(sell("BTC-USDT"), price("110.00"), now.Add(time.Minute))

	snap := a.Snapshot(now)
	if snap.TotalPL.Float() <= 0 {
		t.Errorf("a 10%% move should clear 30bps of costs; got P&L %s", snap.TotalPL)
	}
	if snap.Wins != 1 {
		t.Errorf("Wins = %d, want 1", snap.Wins)
	}
}

func TestBuySlipsUpSellSlipsDown(t *testing.T) {
	a := NewAccount(testConfig(), quietLog())
	now := time.Now().UTC()

	fill, err := a.Apply(buy("BTC-USDT"), price("100.00"), now)
	if err != nil || fill == nil {
		t.Fatalf("buy: %v", err)
	}
	if fill.Price.Float() <= 100 {
		t.Errorf("buy filled at %s, want above the reference price", fill.Price)
	}

	fill, err = a.Apply(sell("BTC-USDT"), price("100.00"), now)
	if err != nil || fill == nil {
		t.Fatalf("sell: %v", err)
	}
	if fill.Price.Float() >= 100 {
		t.Errorf("sell filled at %s, want below the reference price", fill.Price)
	}
}

func TestPositionSizeRespectsMaxFraction(t *testing.T) {
	cfg := testConfig()
	cfg.MaxPositionFraction = 0.25
	a := NewAccount(cfg, quietLog())

	fill, err := a.Apply(buy("BTC-USDT"), price("100.00"), time.Now().UTC())
	if err != nil || fill == nil {
		t.Fatalf("buy: %v", err)
	}

	// 25% of 10,000 equity ≈ 2,500 notional.
	if got := fill.Notional.Float(); got > 2600 || got < 2400 {
		t.Errorf("notional = %.2f, want ~2500 (25%% of equity)", got)
	}
}

func TestNoDoubleEntryAndNoNakedSell(t *testing.T) {
	a := NewAccount(testConfig(), quietLog())
	now := time.Now().UTC()

	// Selling without a position is a no-op: this engine is long-only and
	// must never open a short by accident.
	fill, err := a.Apply(sell("BTC-USDT"), price("100.00"), now)
	if err != nil {
		t.Fatalf("sell with no position: %v", err)
	}
	if fill != nil {
		t.Error("selling with no position produced a fill; the engine must be long-only")
	}

	if _, err := a.Apply(buy("BTC-USDT"), price("100.00"), now); err != nil {
		t.Fatal(err)
	}
	fill, err = a.Apply(buy("BTC-USDT"), price("100.00"), now)
	if err != nil {
		t.Fatal(err)
	}
	if fill != nil {
		t.Error("second buy produced a fill; only one position per instrument is allowed")
	}
}

func TestRejectsNonsensePrice(t *testing.T) {
	a := NewAccount(testConfig(), quietLog())
	now := time.Now().UTC()

	for _, p := range []model.Decimal{{Unscaled: 0, Scale: 2}, {Unscaled: -100, Scale: 2}} {
		if _, err := a.Apply(buy("BTC-USDT"), p, now); err == nil {
			t.Errorf("expected an error trading at price %s", p)
		}
	}
}

func TestCannotSpendMoreThanCash(t *testing.T) {
	cfg := testConfig()
	cfg.MaxPositionFraction = 1.0
	a := NewAccount(cfg, quietLog())
	now := time.Now().UTC()

	// Open positions across several instruments; the account must run out
	// of cash rather than going negative.
	for _, id := range []model.InstrumentID{"BTC-USDT", "ETH-USDT", "SOL-USDT", "XRP-USDT"} {
		if _, err := a.Apply(buy(id), price("100.00"), now); err != nil {
			t.Fatalf("buy %s: %v", id, err)
		}
	}

	snap := a.Snapshot(now)
	if snap.Cash.IsNegative() {
		t.Errorf("cash went negative: %s", snap.Cash)
	}
}

func TestUnrealizedPLTracksMarks(t *testing.T) {
	a := NewAccount(testConfig(), quietLog())
	now := time.Now().UTC()

	if _, err := a.Apply(buy("BTC-USDT"), price("100.00"), now); err != nil {
		t.Fatal(err)
	}
	a.Mark(map[model.InstrumentID]model.Decimal{"BTC-USDT": price("120.00")})

	snap := a.Snapshot(now)
	if snap.UnrealizedPL.Float() <= 0 {
		t.Errorf("UnrealizedPL = %s, want positive after a 20%% mark-up", snap.UnrealizedPL)
	}
	if snap.RealizedPL.Float() != 0 {
		t.Errorf("RealizedPL = %s, want 0 with the position still open", snap.RealizedPL)
	}
}

func TestDrawdownRecorded(t *testing.T) {
	a := NewAccount(testConfig(), quietLog())
	now := time.Now().UTC()

	_, _ = a.Apply(buy("BTC-USDT"), price("100.00"), now)
	a.Mark(map[model.InstrumentID]model.Decimal{"BTC-USDT": price("150.00")}) // peak
	a.Mark(map[model.InstrumentID]model.Decimal{"BTC-USDT": price("75.00")})  // trough

	if dd := a.Snapshot(now).MaxDrawdownPct; dd <= 0 {
		t.Errorf("MaxDrawdownPct = %.2f, want positive after a peak-to-trough move", dd)
	}
}

// TestStateSurvivesRestart matters for a week-long run: a deploy or a crash
// must not silently reset the experiment.
func TestStateSurvivesRestart(t *testing.T) {
	a := NewAccount(testConfig(), quietLog())
	now := time.Now().UTC()

	_, _ = a.Apply(buy("BTC-USDT"), price("100.00"), now)
	_, _ = a.Apply(sell("BTC-USDT"), price("110.00"), now.Add(time.Minute))
	_, _ = a.Apply(buy("ETH-USDT"), price("50.00"), now.Add(2*time.Minute))

	blob, err := a.MarshalState()
	if err != nil {
		t.Fatalf("MarshalState: %v", err)
	}

	restored := NewAccount(testConfig(), quietLog())
	if err := restored.UnmarshalState(blob); err != nil {
		t.Fatalf("UnmarshalState: %v", err)
	}

	before, after := a.Snapshot(now), restored.Snapshot(now)
	if before.Cash.String() != after.Cash.String() {
		t.Errorf("Cash = %s, want %s", after.Cash, before.Cash)
	}
	if before.RealizedPL.String() != after.RealizedPL.String() {
		t.Errorf("RealizedPL = %s, want %s", after.RealizedPL, before.RealizedPL)
	}
	if before.Trades != after.Trades || before.Wins != after.Wins {
		t.Errorf("trades/wins = %d/%d, want %d/%d",
			after.Trades, after.Wins, before.Trades, before.Wins)
	}
	if len(after.Positions) != 1 || after.Positions[0].Instrument != "ETH-USDT" {
		t.Errorf("positions = %+v, want one open ETH-USDT position", after.Positions)
	}
}

func TestFillLogRecordsReason(t *testing.T) {
	a := NewAccount(testConfig(), quietLog())

	sig := buy("BTC-USDT")
	sig.Reason = "fast SMA crossed above slow SMA"
	if _, err := a.Apply(sig, price("100.00"), time.Now().UTC()); err != nil {
		t.Fatal(err)
	}

	fills := a.Fills(10)
	if len(fills) != 1 {
		t.Fatalf("got %d fills, want 1", len(fills))
	}
	// The reason is what makes a week of trades auditable after the fact.
	if fills[0].Reason != sig.Reason {
		t.Errorf("Reason = %q, want %q", fills[0].Reason, sig.Reason)
	}
}

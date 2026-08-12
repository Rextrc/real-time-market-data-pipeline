package alpaca

import (
	"context"
	"testing"

	"github.com/Rextrc/real-time-market-data-pipeline/internal/broker"
	"github.com/Rextrc/real-time-market-data-pipeline/internal/model"
)

// TestBuyAndSellAgainstFakeServer drives the real Client — real HTTP,
// real JSON encoding via the Alpaca SDK, real polling in Submit — against a
// local stand-in for Alpaca's REST API. This is the closest this suite can
// get to proving the wire integration works without reaching Alpaca's
// actual hosts, which this sandbox's network policy blocks outright.
func TestBuyAndSellAgainstFakeServer(t *testing.T) {
	srv := newFakeAlpaca(t)

	c, err := New(Config{APIKey: "k", APISecret: "s", BaseURL: srv.URL}, testRegistry{}, quietLog())
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	ctx := context.Background()
	if err := c.Ping(ctx); err != nil {
		t.Fatalf("Ping: %v", err)
	}

	balances, err := c.Balances(ctx)
	if err != nil {
		t.Fatalf("Balances: %v", err)
	}
	if got := usdOf(balances); got != "10000" {
		t.Errorf("starting cash = %q, want 10000", got)
	}

	buyAmt := model.FromFloat(2500, 2)
	fill, err := c.Submit(ctx, broker.Order{
		Instrument: "BTC-USDT", Side: broker.Buy, QuoteAmount: buyAmt,
		ClientID: "test-buy-1", Reason: "integration test",
	})
	if err != nil {
		t.Fatalf("Submit buy: %v", err)
	}
	if fill.Quantity.IsZero() {
		t.Error("buy fill has zero quantity")
	}
	// 2500 / 100 = 25 BTC at the fake server's fixed price.
	if got := fill.Quantity.Float(); got < 24.9 || got > 25.1 {
		t.Errorf("filled quantity = %v, want ~25", got)
	}
	if fill.OrderID == "" {
		t.Error("fill has no order ID")
	}

	balances, err = c.Balances(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if got := usdOf(balances); got == "10000" {
		t.Error("cash did not decrease after the buy")
	}
	var heldQty model.Decimal
	for _, b := range balances {
		if b.Asset == "BTC" { // broker.Balance.Asset is the base asset, matching InstrumentID's BASE-QUOTE split
			heldQty = b.Free
		}
	}
	if heldQty.IsZero() {
		t.Fatal("no BTC position after the buy")
	}

	sellFill, err := c.Submit(ctx, broker.Order{
		Instrument: "BTC-USDT", Side: broker.Sell, Quantity: heldQty,
		ClientID: "test-sell-1", Reason: "closing",
	})
	if err != nil {
		t.Fatalf("Submit sell: %v", err)
	}
	if sellFill.Side != broker.Sell {
		t.Errorf("Side = %v, want Sell", sellFill.Side)
	}

	balances, err = c.Balances(ctx)
	if err != nil {
		t.Fatal(err)
	}
	for _, b := range balances {
		if b.Asset == "BTC" && !b.Free.IsZero() {
			t.Errorf("still holding %s BTC after selling the full position", b.Free)
		}
	}
}

func usdOf(balances []broker.Balance) string {
	for _, b := range balances {
		if b.Asset == "USD" {
			return b.Free.String()
		}
	}
	return ""
}

type testRegistry struct{}

func (testRegistry) Symbol(venue model.VenueID, id model.InstrumentID) (string, bool) {
	return "BTCUSDT", true
}

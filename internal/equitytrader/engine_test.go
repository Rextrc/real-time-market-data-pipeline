package equitytrader

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"sync"
	"testing"

	"github.com/Rextrc/real-time-market-data-pipeline/internal/broker"
	"github.com/Rextrc/real-time-market-data-pipeline/internal/broker/alpaca"
	"github.com/Rextrc/real-time-market-data-pipeline/internal/model"
	"github.com/Rextrc/real-time-market-data-pipeline/internal/strategy"
)

func noopLogger() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

// fakeTrader is an in-memory Trader for tests — no network, deterministic.
type fakeTrader struct {
	mu        sync.Mutex
	quotes    map[string]float64
	positions []alpaca.PositionInfo
	equity    float64
	orders    []alpaca.EquityOrder
	orderErr  error
}

func (f *fakeTrader) Name() string { return "fake" }

func (f *fakeTrader) Quote(ctx context.Context, symbol string) (model.Decimal, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	p, ok := f.quotes[symbol]
	if !ok {
		return model.Decimal{}, errors.New("no quote")
	}
	return model.FromFloat(p, 4), nil
}

func (f *fakeTrader) AccountSummary(ctx context.Context) (*alpaca.AccountSummary, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return &alpaca.AccountSummary{PortfolioValue: model.FromFloat(f.equity, 2)}, nil
}

func (f *fakeTrader) Positions(ctx context.Context) ([]alpaca.PositionInfo, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]alpaca.PositionInfo(nil), f.positions...), nil
}

func (f *fakeTrader) PlaceEquityOrder(ctx context.Context, o alpaca.EquityOrder) (*broker.Fill, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.orderErr != nil {
		return nil, f.orderErr
	}
	f.orders = append(f.orders, o)
	price := f.quotes[o.Symbol]
	qty := o.Qty
	if qty.IsZero() {
		qty = model.FromFloat(o.Notional.Float()/price, 6)
	}
	return &broker.Fill{Instrument: model.InstrumentID(o.Symbol), Side: o.Side,
		Price: model.FromFloat(price, 4), Quantity: qty}, nil
}

func TestEngine_PollBuildsHistory(t *testing.T) {
	trader := &fakeTrader{quotes: map[string]float64{"AAPL": 150}}
	e := NewEngine(Config{Symbols: []string{"AAPL"}}, trader, strategy.NewMomentum(1, 2), noopLogger())

	e.poll(context.Background())
	e.poll(context.Background())

	bars := e.Candles("AAPL", 0)
	if len(bars) != 2 {
		t.Fatalf("expected 2 bars after 2 polls, got %d", len(bars))
	}
	last, ok := e.Last("AAPL")
	if !ok || last.Price.Float() != 150 {
		t.Fatalf("Last = %+v, ok=%v", last, ok)
	}
}

func TestEngine_BuySizesFromEquityAndSkipsIfAlreadyHeld(t *testing.T) {
	trader := &fakeTrader{quotes: map[string]float64{"AAPL": 100}, equity: 10000}
	e := NewEngine(Config{Symbols: []string{"AAPL"}, MaxPositionFraction: 0.1}, trader, strategy.NewMomentum(1, 2), noopLogger())

	sig := strategy.Signal{Instrument: "AAPL", Action: strategy.ActionBuy, Strength: 1, Reason: "test"}
	e.buy(context.Background(), sig)

	trader.mu.Lock()
	n := len(trader.orders)
	last := trader.orders[len(trader.orders)-1]
	trader.mu.Unlock()

	if n != 1 {
		t.Fatalf("expected 1 order, got %d", n)
	}
	if last.Notional.Float() != 1000 { // 10000 * 0.1
		t.Fatalf("notional = %v, want 1000", last.Notional.Float())
	}

	// Already held — a second buy signal must not submit another order.
	e.buy(context.Background(), sig)
	trader.mu.Lock()
	n2 := len(trader.orders)
	trader.mu.Unlock()
	if n2 != 1 {
		t.Fatalf("expected no additional order once held, got %d total", n2)
	}
}

func TestEngine_SellSkipsIfNotHeld(t *testing.T) {
	trader := &fakeTrader{quotes: map[string]float64{"AAPL": 100}}
	e := NewEngine(Config{Symbols: []string{"AAPL"}}, trader, strategy.NewMomentum(1, 2), noopLogger())

	e.sell(context.Background(), strategy.Signal{Instrument: "AAPL", Action: strategy.ActionSell})

	trader.mu.Lock()
	n := len(trader.orders)
	trader.mu.Unlock()
	if n != 0 {
		t.Fatalf("expected no order for a sell with nothing held, got %d", n)
	}
}

func TestEngine_SellSubmitsHeldQuantity(t *testing.T) {
	trader := &fakeTrader{
		quotes:    map[string]float64{"AAPL": 120},
		positions: []alpaca.PositionInfo{{Symbol: "AAPL", Qty: model.FromFloat(3, 6)}},
	}
	e := NewEngine(Config{Symbols: []string{"AAPL"}}, trader, strategy.NewMomentum(1, 2), noopLogger())
	e.syncHeld(context.Background())

	e.sell(context.Background(), strategy.Signal{Instrument: "AAPL", Action: strategy.ActionSell, Reason: "test"})

	trader.mu.Lock()
	defer trader.mu.Unlock()
	if len(trader.orders) != 1 {
		t.Fatalf("expected 1 order, got %d", len(trader.orders))
	}
	if trader.orders[0].Qty.Float() != 3 {
		t.Fatalf("qty = %v, want 3", trader.orders[0].Qty.Float())
	}
}

func TestEngine_InstrumentsMatchesSymbols(t *testing.T) {
	trader := &fakeTrader{quotes: map[string]float64{}}
	e := NewEngine(Config{Symbols: []string{"AAPL", "TSLA"}}, trader, strategy.NewMomentum(1, 2), noopLogger())
	got := e.Instruments()
	if len(got) != 2 || got[0] != "AAPL" || got[1] != "TSLA" {
		t.Fatalf("got %+v", got)
	}
}

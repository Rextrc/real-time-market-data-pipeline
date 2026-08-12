package live

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/Rextrc/real-time-market-data-pipeline/internal/broker"
	busiface "github.com/Rextrc/real-time-market-data-pipeline/internal/bus"
	"github.com/Rextrc/real-time-market-data-pipeline/internal/bus/inproc"
	"github.com/Rextrc/real-time-market-data-pipeline/internal/consumer/candles"
	"github.com/Rextrc/real-time-market-data-pipeline/internal/model"
	"github.com/Rextrc/real-time-market-data-pipeline/internal/strategy"
)

func quietLog() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

// fakeBroker is an in-memory broker.Broker for engine tests. It tracks USD
// cash and per-symbol holdings so the one-position and sizing rules can be
// exercised without a network.
type fakeBroker struct {
	mu         sync.Mutex
	cash       model.Decimal
	holdings   map[string]model.Decimal
	submits    []broker.Order
	rejectNext bool
}

func newFakeBroker(cash float64) *fakeBroker {
	return &fakeBroker{cash: model.FromFloat(cash, 2), holdings: map[string]model.Decimal{}}
}

func (f *fakeBroker) Name() string                   { return "fake" }
func (f *fakeBroker) Ping(ctx context.Context) error { return nil }

func (f *fakeBroker) Balances(ctx context.Context) ([]broker.Balance, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := []broker.Balance{{Asset: "USD", Free: f.cash}}
	for asset, qty := range f.holdings {
		if !qty.IsZero() {
			out = append(out, broker.Balance{Asset: asset, Free: qty})
		}
	}
	return out, nil
}

func (f *fakeBroker) Submit(ctx context.Context, o broker.Order) (*broker.Fill, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.submits = append(f.submits, o)

	if f.rejectNext {
		f.rejectNext = false
		return nil, broker.ErrRejected
	}

	base, _, _ := splitInstrument(o.Instrument)
	const price = 100.0 // fixed fake price, in dollars

	switch o.Side {
	case broker.Buy:
		spend := o.QuoteAmount.Float()
		if spend > f.cash.Float() {
			return nil, broker.ErrInsufficientFunds
		}
		qty := model.FromFloat(spend/price, 8)
		f.cash = f.cash.Sub(o.QuoteAmount)
		f.holdings[base] = f.holdings[base].Add(qty)
		return &broker.Fill{
			Instrument: o.Instrument, Side: o.Side,
			Price: model.FromFloat(price, 2), Quantity: qty, QuoteSpent: o.QuoteAmount,
			ClientID: o.ClientID, Time: time.Now().UTC(), Reason: o.Reason,
		}, nil

	case broker.Sell:
		qty := o.Quantity
		proceeds := model.FromFloat(qty.Float()*price, 2)
		f.cash = f.cash.Add(proceeds)
		f.holdings[base] = model.Decimal{}
		return &broker.Fill{
			Instrument: o.Instrument, Side: o.Side,
			Price: model.FromFloat(price, 2), Quantity: qty, QuoteSpent: proceeds,
			ClientID: o.ClientID, Time: time.Now().UTC(), Reason: o.Reason,
		}, nil
	}
	return nil, errors.New("unsupported side")
}

func splitInstrument(id model.InstrumentID) (base, quote string, ok bool) {
	s := string(id)
	for i := 0; i < len(s); i++ {
		if s[i] == '-' {
			return s[:i], s[i+1:], true
		}
	}
	return s, "", false
}

// fixedStrategy emits a scripted sequence of signal batches, one batch per
// Evaluate call, so tests can drive the engine deterministically.
type fixedStrategy struct {
	mu      sync.Mutex
	batches [][]strategy.Signal
	i       int
}

func (s *fixedStrategy) Name() string { return "fixed" }
func (s *fixedStrategy) Evaluate(ctx context.Context, m strategy.Market) ([]strategy.Signal, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.i >= len(s.batches) {
		return nil, nil
	}
	b := s.batches[s.i]
	s.i++
	return b, nil
}

func mkTick(id model.InstrumentID, price float64) model.Tick {
	return model.Tick{
		Venue: model.VenueBinance, Instrument: id,
		Price: model.FromFloat(price, 2), Quantity: model.FromFloat(1, 8),
		EventTime: time.Now().UTC(), RecvTime: time.Now().UTC(),
	}
}

// runEngine wires an Engine to an in-proc bus, publishes a tick to warm
// Last(), then drives one full eval tick and returns once at least one
// evaluation has happened.
func runEngine(t *testing.T, e *Engine, b *inproc.Bus, sub busiface.Subscription, ticks int) {
	t.Helper()

	// Long enough for a couple of eval ticks at the intervals these tests
	// use, short enough that a hang fails fast rather than burning a full
	// context timeout waiting for something that was never going to happen.
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()

	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = e.Run(ctx, sub)
	}()

	// Give Run a moment to reach Ping/syncHeldPositions before publishing.
	time.Sleep(20 * time.Millisecond)
	for i := 0; i < ticks; i++ {
		_ = b.Publish(ctx, []model.Tick{mkTick("BTC-USDT", 100)})
	}

	// Stop as soon as the strategy's scripted signal has been consumed,
	// rather than waiting out the full context — sub.Done() is not enough
	// signal on its own, so poll the fill/submit log growing.
	deadline := time.After(400 * time.Millisecond)
	for {
		select {
		case <-done:
			return
		case <-deadline:
			cancel()
			<-done
			return
		default:
			if len(e.Fills(1)) > 0 {
				cancel()
				<-done
				return
			}
			time.Sleep(5 * time.Millisecond)
		}
	}
}

func TestBuySizesFromEquityAndStrength(t *testing.T) {
	fb := newFakeBroker(10000)
	strat := &fixedStrategy{batches: [][]strategy.Signal{
		{{Instrument: "BTC-USDT", Action: strategy.ActionBuy, Strength: 0.5, Reason: "test"}},
	}}
	b := inproc.New()
	defer b.Close()
	sub, err := b.Subscribe(busiface.SubscriberSpec{Name: "live", Capacity: 8, Policy: busiface.PolicyCoalesce})
	if err != nil {
		t.Fatal(err)
	}

	e := NewEngine(Config{EvalInterval: 50 * time.Millisecond, MaxPositionFraction: 0.5},
		fb, strat, candles.New(time.Minute, 10), model.VenueBinance, []model.InstrumentID{"BTC-USDT"}, quietLog())

	runEngine(t, e, b, sub, 1)

	fills := e.Fills(10)
	if len(fills) != 1 {
		t.Fatalf("got %d fills, want 1", len(fills))
	}
	// 50% max position * 0.5 strength * 10000 equity = 2500.
	if got := fills[0].QuoteSpent.Float(); got < 2400 || got > 2600 {
		t.Errorf("quote spent = %.2f, want ~2500 (maxFraction * strength * equity)", got)
	}
}

func TestOnePositionPerInstrument(t *testing.T) {
	fb := newFakeBroker(10000)
	strat := &fixedStrategy{batches: [][]strategy.Signal{
		{{Instrument: "BTC-USDT", Action: strategy.ActionBuy, Strength: 1, Reason: "first"}},
		{{Instrument: "BTC-USDT", Action: strategy.ActionBuy, Strength: 1, Reason: "second"}},
	}}
	b := inproc.New()
	defer b.Close()
	sub, err := b.Subscribe(busiface.SubscriberSpec{Name: "live", Capacity: 8, Policy: busiface.PolicyCoalesce})
	if err != nil {
		t.Fatal(err)
	}

	e := NewEngine(Config{EvalInterval: 40 * time.Millisecond, MaxPositionFraction: 0.1},
		fb, strat, candles.New(time.Minute, 10), model.VenueBinance, []model.InstrumentID{"BTC-USDT"}, quietLog())

	ctx, cancel := context.WithTimeout(context.Background(), 400*time.Millisecond)
	defer cancel()
	done := make(chan struct{})
	go func() { defer close(done); _ = e.Run(ctx, sub) }()
	time.Sleep(20 * time.Millisecond)
	for i := 0; i < 5; i++ {
		_ = b.Publish(ctx, []model.Tick{mkTick("BTC-USDT", 100)})
		time.Sleep(60 * time.Millisecond)
	}
	<-done

	if got := len(fb.submits); got != 1 {
		t.Errorf("broker saw %d order submissions, want 1 — a second buy while already"+
			" holding the instrument must never reach the broker", got)
	}
}

func TestSellWithNoPositionNeverSubmits(t *testing.T) {
	fb := newFakeBroker(10000)
	strat := &fixedStrategy{batches: [][]strategy.Signal{
		{{Instrument: "BTC-USDT", Action: strategy.ActionSell, Strength: 1, Reason: "no position"}},
	}}
	b := inproc.New()
	defer b.Close()
	sub, err := b.Subscribe(busiface.SubscriberSpec{Name: "live", Capacity: 8, Policy: busiface.PolicyCoalesce})
	if err != nil {
		t.Fatal(err)
	}

	e := NewEngine(Config{EvalInterval: 40 * time.Millisecond},
		fb, strat, candles.New(time.Minute, 10), model.VenueBinance, []model.InstrumentID{"BTC-USDT"}, quietLog())

	runEngine(t, e, b, sub, 1)

	if len(fb.submits) != 0 {
		t.Errorf("broker saw %d submissions, want 0 — selling with no position must be a no-op", len(fb.submits))
	}
}

// TestFailedOrderDoesNotMarkPositionHeld checks that a rejected buy leaves
// the engine free to try again rather than believing it holds a position it
// never actually acquired.
func TestFailedOrderDoesNotMarkPositionHeld(t *testing.T) {
	fb := newFakeBroker(10000)
	fb.rejectNext = true
	strat := &fixedStrategy{batches: [][]strategy.Signal{
		{{Instrument: "BTC-USDT", Action: strategy.ActionBuy, Strength: 1, Reason: "will fail"}},
		{{Instrument: "BTC-USDT", Action: strategy.ActionBuy, Strength: 1, Reason: "retry"}},
	}}
	b := inproc.New()
	defer b.Close()
	sub, err := b.Subscribe(busiface.SubscriberSpec{Name: "live", Capacity: 8, Policy: busiface.PolicyCoalesce})
	if err != nil {
		t.Fatal(err)
	}

	e := NewEngine(Config{EvalInterval: 40 * time.Millisecond, MaxPositionFraction: 0.1},
		fb, strat, candles.New(time.Minute, 10), model.VenueBinance, []model.InstrumentID{"BTC-USDT"}, quietLog())

	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()
	done := make(chan struct{})
	go func() { defer close(done); _ = e.Run(ctx, sub) }()
	time.Sleep(20 * time.Millisecond)
	for i := 0; i < 4; i++ {
		_ = b.Publish(ctx, []model.Tick{mkTick("BTC-USDT", 100)})
		time.Sleep(60 * time.Millisecond)
	}
	<-done

	if len(e.Fills(10)) != 1 {
		t.Errorf("got %d fills, want 1 (first rejected, second should succeed)", len(e.Fills(10)))
	}
	if len(fb.submits) != 2 {
		t.Errorf("broker saw %d submissions, want 2 — a rejected order must not block the retry", len(fb.submits))
	}
}

// Package live drives a strategy against a real broker.
//
// This is the execution path the rest of the system is built to be
// cautious about. Where internal/consumer/paper simulates fills locally,
// this package submits real orders through internal/broker and reads the
// resulting position and P&L back from the broker's own account — Alpaca is
// the ledger here, not a local one. Everything upstream of Submit (the
// strategy, the risk cap, the client-order-id) is unchanged from paper
// trading; only what happens to the order past that boundary differs.
package live

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/Rextrc/real-time-market-data-pipeline/internal/broker"
	"github.com/Rextrc/real-time-market-data-pipeline/internal/bus"
	"github.com/Rextrc/real-time-market-data-pipeline/internal/consumer/candles"
	"github.com/Rextrc/real-time-market-data-pipeline/internal/model"
	"github.com/Rextrc/real-time-market-data-pipeline/internal/strategy"
)

// Config tunes the trading loop. Fields mirror paper.EngineConfig
// deliberately, so switching between simulated and real execution is a
// wiring change, not a rethink of the schedule.
type Config struct {
	Name string
	// EvalInterval — see paper.EngineConfig for why this must not be
	// tick-driven. It matters even more here: every evaluation that trades
	// is a real order, so the interval is also the ceiling on how often the
	// account can be traded.
	EvalInterval time.Duration
	// MaxPositionFraction caps one order's notional as a fraction of the
	// account's total equity, computed from the broker's own balances. This
	// is enforced here, independent of anything the strategy or the broker
	// itself would allow, because it is the one number that determines how
	// bad a bug in either of them can get.
	MaxPositionFraction float64
}

func DefaultConfig() Config {
	return Config{EvalInterval: 60 * time.Second, MaxPositionFraction: 0.1}
}

// Engine runs a strategy against live prices and submits real orders.
type Engine struct {
	cfg     Config
	broker  broker.Broker
	strat   strategy.Strategy
	builder *candles.Builder
	venue   model.VenueID
	log     *slog.Logger

	mu       sync.RWMutex
	last     map[model.InstrumentID]model.Tick
	insts    []model.InstrumentID
	held     map[model.InstrumentID]bool // instruments this engine currently holds, to stay long-only/one-position
	fillLog  []broker.Fill
	failures int
}

func NewEngine(cfg Config, b broker.Broker, strat strategy.Strategy, builder *candles.Builder,
	venue model.VenueID, instruments []model.InstrumentID, log *slog.Logger) *Engine {
	d := DefaultConfig()
	if cfg.EvalInterval <= 0 {
		cfg.EvalInterval = d.EvalInterval
	}
	if cfg.MaxPositionFraction <= 0 {
		cfg.MaxPositionFraction = d.MaxPositionFraction
	}
	return &Engine{
		cfg: cfg, broker: b, strat: strat, builder: builder, venue: venue, log: log,
		last:  make(map[model.InstrumentID]model.Tick),
		insts: instruments,
		held:  make(map[model.InstrumentID]bool),
	}
}

// --- strategy.Market ---------------------------------------------------

func (e *Engine) Last(id model.InstrumentID) (model.Tick, bool) {
	e.mu.RLock()
	defer e.mu.RUnlock()
	t, ok := e.last[id]
	return t, ok
}

func (e *Engine) Candles(id model.InstrumentID, limit int) []candles.Candle {
	return e.builder.Recent(e.venue, id, limit)
}

func (e *Engine) Instruments() []model.InstrumentID {
	e.mu.RLock()
	defer e.mu.RUnlock()
	out := make([]model.InstrumentID, len(e.insts))
	copy(out, e.insts)
	return out
}

var _ strategy.Market = (*Engine)(nil)

// -------------------------------------------------------------------------

func (e *Engine) Name() string {
	if e.cfg.Name != "" {
		return e.cfg.Name
	}
	return e.strat.Name()
}

func (e *Engine) BrokerName() string { return e.broker.Name() }

// Fills returns the trade log, most recent last.
func (e *Engine) Fills(limit int) []broker.Fill {
	e.mu.RLock()
	defer e.mu.RUnlock()
	if limit <= 0 || limit > len(e.fillLog) {
		limit = len(e.fillLog)
	}
	out := make([]broker.Fill, limit)
	copy(out, e.fillLog[len(e.fillLog)-limit:])
	return out
}

// Run connects to the broker, then consumes ticks and evaluates the strategy
// on a timer for as long as ctx is live.
func (e *Engine) Run(ctx context.Context, sub bus.Subscription) error {
	if err := e.broker.Ping(ctx); err != nil {
		return fmt.Errorf("live: %s: broker not reachable: %w", e.Name(), err)
	}
	e.syncHeldPositions(ctx)

	evalTicker := time.NewTicker(e.cfg.EvalInterval)
	defer evalTicker.Stop()

	for {
		select {
		case <-ctx.Done():
			return nil
		case <-sub.Done():
			return nil

		case t := <-sub.Ticks():
			e.mu.Lock()
			e.last[t.Instrument] = t
			e.mu.Unlock()

		case <-evalTicker.C:
			e.evaluate(ctx)
		}
	}
}

// syncHeldPositions reads what the account actually holds before the first
// evaluation. Without this, a restart forgets every open position and the
// engine can double up on an instrument the previous run already bought.
func (e *Engine) syncHeldPositions(ctx context.Context) {
	balances, err := e.broker.Balances(ctx)
	if err != nil {
		e.log.Warn("could not read starting positions; assuming none held", "err", err)
		return
	}

	held := make(map[model.InstrumentID]bool)
	for _, id := range e.insts {
		for _, b := range balances {
			if !b.Free.IsZero() && matchesAsset(id, b.Asset) {
				held[id] = true
			}
		}
	}

	e.mu.Lock()
	e.held = held
	e.mu.Unlock()

	if len(held) > 0 {
		e.log.Info("resumed with existing positions", "instruments", keys(held))
	}
}

func matchesAsset(id model.InstrumentID, asset string) bool {
	// A loose match is enough here: this only gates "do we already hold
	// this", not order sizing, and instrument IDs are BASE-QUOTE while
	// broker balances are keyed by base asset alone.
	s := string(id)
	for i := 0; i < len(s); i++ {
		if s[i] == '-' {
			return s[:i] == asset
		}
	}
	return false
}

func (e *Engine) evaluate(ctx context.Context) {
	sigs, err := e.strat.Evaluate(ctx, e)
	if err != nil {
		e.log.Warn("strategy evaluation failed", "strategy", e.strat.Name(), "err", err)
		return
	}

	for _, sig := range sigs {
		e.apply(ctx, sig)
	}
}

func (e *Engine) apply(ctx context.Context, sig strategy.Signal) {
	switch sig.Action {
	case strategy.ActionBuy:
		e.buy(ctx, sig)
	case strategy.ActionSell:
		e.sell(ctx, sig)
	}
}

func (e *Engine) buy(ctx context.Context, sig strategy.Signal) {
	e.mu.RLock()
	already := e.held[sig.Instrument]
	e.mu.RUnlock()
	if already {
		return // one position per instrument, same rule as paper trading
	}

	balances, err := e.broker.Balances(ctx)
	if err != nil {
		e.log.Warn("could not read balances; skipping buy", "instrument", sig.Instrument, "err", err)
		return
	}
	equity := totalUSD(balances)
	if equity.IsZero() {
		e.log.Warn("account equity read as zero; skipping buy", "instrument", sig.Instrument)
		return
	}

	notional := model.FromFloat(equity.Float()*e.cfg.MaxPositionFraction*clamp01(sig.Strength), 2)
	if notional.Float() <= 0 {
		return
	}

	order := broker.Order{
		Instrument:  sig.Instrument,
		Side:        broker.Buy,
		QuoteAmount: notional,
		ClientID:    clientOrderID(),
		Reason:      sig.Reason,
	}
	e.submit(ctx, order, sig)
}

func (e *Engine) sell(ctx context.Context, sig strategy.Signal) {
	e.mu.RLock()
	held := e.held[sig.Instrument]
	e.mu.RUnlock()
	if !held {
		return // nothing to close, mirrors paper trading's long-only rule
	}

	balances, err := e.broker.Balances(ctx)
	if err != nil {
		e.log.Warn("could not read balances; skipping sell", "instrument", sig.Instrument, "err", err)
		return
	}

	var qty model.Decimal
	for _, b := range balances {
		if matchesAsset(sig.Instrument, b.Asset) {
			qty = b.Free
		}
	}
	if qty.IsZero() {
		return
	}

	order := broker.Order{
		Instrument: sig.Instrument,
		Side:       broker.Sell,
		Quantity:   qty,
		ClientID:   clientOrderID(),
		Reason:     sig.Reason,
	}
	e.submit(ctx, order, sig)
}

func (e *Engine) submit(ctx context.Context, order broker.Order, sig strategy.Signal) {
	fill, err := e.broker.Submit(ctx, order)
	if err != nil {
		e.mu.Lock()
		e.failures++
		e.mu.Unlock()
		e.log.Warn("order failed", "instrument", order.Instrument, "side", order.Side, "err", err)
		return
	}

	e.mu.Lock()
	e.fillLog = append(e.fillLog, *fill)
	e.held[order.Instrument] = order.Side == broker.Buy
	e.mu.Unlock()

	e.log.Info("live fill",
		"broker", e.broker.Name(), "side", fill.Side, "instrument", fill.Instrument,
		"price", fill.Price.String(), "qty", fill.Quantity.String(),
		"quote_spent", fill.QuoteSpent.String(), "partial", fill.Partial,
		"reason", fill.Reason)
}

func totalUSD(balances []broker.Balance) model.Decimal {
	for _, b := range balances {
		if b.Asset == "USD" {
			return b.Free
		}
	}
	return model.Decimal{}
}

func clientOrderID() string {
	var b [12]byte
	_, _ = rand.Read(b[:])
	return "mdp-" + hex.EncodeToString(b[:])
}

func clamp01(f float64) float64 {
	switch {
	case f <= 0:
		return 0
	case f > 1:
		return 1
	default:
		return f
	}
}

func keys(m map[model.InstrumentID]bool) []model.InstrumentID {
	out := make([]model.InstrumentID, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	return out
}

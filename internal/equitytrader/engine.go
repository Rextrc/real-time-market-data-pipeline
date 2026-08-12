// Package equitytrader automatically trades a basket of stocks through
// Alpaca.
//
// There is no live equity tick stream anywhere else in this system —
// internal/consumer/live drives the crypto pipeline off Binance's
// websocket. Building a second websocket pipeline just for a stock basket
// was more infrastructure than this needed, so this engine instead builds
// its own short price history by polling Alpaca's quote endpoint on a
// timer, and feeds that history to the same strategy.Strategy interface
// (typically strategy.Adaptive) everything else in the system uses — the
// "which arm is winning" logic doesn't need a second implementation, only
// a second way of getting it prices.
package equitytrader

import (
	"context"
	crand "crypto/rand"
	"encoding/hex"
	"log/slog"
	"strings"
	"sync"
	"time"

	"github.com/Rextrc/real-time-market-data-pipeline/internal/broker"
	"github.com/Rextrc/real-time-market-data-pipeline/internal/broker/alpaca"
	"github.com/Rextrc/real-time-market-data-pipeline/internal/consumer/candles"
	"github.com/Rextrc/real-time-market-data-pipeline/internal/model"
	"github.com/Rextrc/real-time-market-data-pipeline/internal/strategy"
)

// Trader is the subset of *alpaca.Client this engine drives, kept as an
// interface so it can be tested without a real Alpaca connection.
type Trader interface {
	Name() string
	PlaceEquityOrder(ctx context.Context, o alpaca.EquityOrder) (*broker.Fill, error)
	Quote(ctx context.Context, symbol string) (model.Decimal, error)
	AccountSummary(ctx context.Context) (*alpaca.AccountSummary, error)
	Positions(ctx context.Context) ([]alpaca.PositionInfo, error)
}

// Config tunes the engine.
type Config struct {
	// Symbols is the traded basket.
	Symbols []string
	// PollInterval is how often each symbol's quote is sampled into the
	// in-memory price history this engine builds for itself.
	PollInterval time.Duration
	// EvalInterval is how often the strategy is asked for signals — same
	// reasoning as paper.EngineConfig.EvalInterval: tying it to every quote
	// poll would trade on noise, not signal.
	EvalInterval time.Duration
	// MaxPositionFraction caps one order's notional as a fraction of
	// account equity. Kept smaller than the crypto engines' default on
	// purpose: this basket can hold several symbols at once, so each one
	// gets a smaller slice.
	MaxPositionFraction float64
	// HistoryBars is how many polled samples are kept per symbol.
	HistoryBars int
}

func DefaultConfig() Config {
	return Config{
		Symbols: []string{
			"AAPL", "MSFT", "NVDA", "TSLA", "AMD",
			"META", "AMZN", "GOOGL", "NFLX", "COIN",
		},
		PollInterval:        30 * time.Second,
		EvalInterval:        60 * time.Second,
		MaxPositionFraction: 0.05,
		HistoryBars:         60,
	}
}

// Engine polls quotes, evaluates a strategy on a timer, and applies fills.
// It is also the strategy.Market implementation the strategy sees.
type Engine struct {
	cfg    Config
	trader Trader
	strat  strategy.Strategy
	log    *slog.Logger

	mu   sync.RWMutex
	hist map[model.InstrumentID][]candles.Candle
	held map[model.InstrumentID]bool
}

func NewEngine(cfg Config, trader Trader, strat strategy.Strategy, log *slog.Logger) *Engine {
	d := DefaultConfig()
	if len(cfg.Symbols) == 0 {
		cfg.Symbols = d.Symbols
	}
	if cfg.PollInterval <= 0 {
		cfg.PollInterval = d.PollInterval
	}
	if cfg.EvalInterval <= 0 {
		cfg.EvalInterval = d.EvalInterval
	}
	if cfg.MaxPositionFraction <= 0 {
		cfg.MaxPositionFraction = d.MaxPositionFraction
	}
	if cfg.HistoryBars <= 0 {
		cfg.HistoryBars = d.HistoryBars
	}
	return &Engine{
		cfg: cfg, trader: trader, strat: strat, log: log,
		hist: make(map[model.InstrumentID][]candles.Candle),
		held: make(map[model.InstrumentID]bool),
	}
}

// --- strategy.Market ---------------------------------------------------

func (e *Engine) Last(id model.InstrumentID) (model.Tick, bool) {
	e.mu.RLock()
	defer e.mu.RUnlock()
	bars := e.hist[id]
	if len(bars) == 0 {
		return model.Tick{}, false
	}
	b := bars[len(bars)-1]
	return model.Tick{Instrument: id, Price: b.Close, EventTime: b.CloseTime}, true
}

func (e *Engine) Candles(id model.InstrumentID, limit int) []candles.Candle {
	e.mu.RLock()
	defer e.mu.RUnlock()
	bars := e.hist[id]
	if limit > 0 && limit < len(bars) {
		bars = bars[len(bars)-limit:]
	}
	return append([]candles.Candle(nil), bars...)
}

func (e *Engine) Instruments() []model.InstrumentID {
	out := make([]model.InstrumentID, len(e.cfg.Symbols))
	for i, s := range e.cfg.Symbols {
		out[i] = model.InstrumentID(s)
	}
	return out
}

var _ strategy.Market = (*Engine)(nil)

// -------------------------------------------------------------------------

func (e *Engine) Name() string { return "equity-" + e.strat.Name() }

// Run polls quotes and evaluates the strategy on independent timers until
// ctx is cancelled.
func (e *Engine) Run(ctx context.Context) error {
	e.syncHeld(ctx)

	pollTicker := time.NewTicker(e.cfg.PollInterval)
	defer pollTicker.Stop()
	evalTicker := time.NewTicker(e.cfg.EvalInterval)
	defer evalTicker.Stop()

	// Seed history immediately rather than waiting a full poll interval, so
	// the first evaluation isn't working from nothing.
	e.poll(ctx)

	for {
		select {
		case <-ctx.Done():
			return nil
		case <-pollTicker.C:
			e.poll(ctx)
		case <-evalTicker.C:
			e.evaluate(ctx)
		}
	}
}

func (e *Engine) poll(ctx context.Context) {
	for _, sym := range e.cfg.Symbols {
		id := model.InstrumentID(sym)
		price, err := e.trader.Quote(ctx, sym)
		if err != nil {
			e.log.Warn("equity quote failed", "symbol", sym, "err", err)
			continue
		}
		if price.IsZero() {
			continue
		}

		now := time.Now().UTC()
		bar := candles.Candle{
			Instrument: id, OpenTime: now, CloseTime: now,
			Open: price, High: price, Low: price, Close: price, Complete: true,
		}

		e.mu.Lock()
		h := append(e.hist[id], bar)
		if len(h) > e.cfg.HistoryBars {
			h = h[len(h)-e.cfg.HistoryBars:]
		}
		e.hist[id] = h
		e.mu.Unlock()
	}
}

// syncHeld reads what the account actually holds before the first
// evaluation, so a restart doesn't forget an open position and double up on
// a symbol a previous run already bought.
func (e *Engine) syncHeld(ctx context.Context) {
	positions, err := e.trader.Positions(ctx)
	if err != nil {
		e.log.Warn("could not read starting equity positions; assuming none held", "err", err)
		return
	}
	held := make(map[model.InstrumentID]bool)
	for _, p := range positions {
		if !p.Qty.IsZero() {
			held[model.InstrumentID(p.Symbol)] = true
		}
	}
	e.mu.Lock()
	e.held = held
	e.mu.Unlock()
}

func (e *Engine) evaluate(ctx context.Context) {
	sigs, err := e.strat.Evaluate(ctx, e)
	if err != nil {
		e.log.Warn("equity strategy evaluation failed", "err", err)
		return
	}
	for _, sig := range sigs {
		switch sig.Action {
		case strategy.ActionBuy:
			e.buy(ctx, sig)
		case strategy.ActionSell:
			e.sell(ctx, sig)
		}
	}
}

func (e *Engine) buy(ctx context.Context, sig strategy.Signal) {
	e.mu.RLock()
	already := e.held[sig.Instrument]
	e.mu.RUnlock()
	if already {
		return // one position per symbol, same rule as the crypto engines
	}

	summary, err := e.trader.AccountSummary(ctx)
	if err != nil {
		e.log.Warn("could not read account; skipping buy", "symbol", sig.Instrument, "err", err)
		return
	}
	equity := summary.PortfolioValue.Float()
	if equity <= 0 {
		return
	}

	notional := equity * e.cfg.MaxPositionFraction * clamp01(sig.Strength)
	if notional <= 0 {
		return
	}

	fill, err := e.trader.PlaceEquityOrder(ctx, alpaca.EquityOrder{
		Symbol: string(sig.Instrument), Side: broker.Buy,
		Notional: model.FromFloat(notional, 2),
		ClientID: clientOrderID(), Reason: sig.Reason,
	})
	if err != nil {
		e.log.Warn("equity order failed", "symbol", sig.Instrument, "side", "buy", "err", err)
		return
	}

	e.mu.Lock()
	e.held[sig.Instrument] = true
	e.mu.Unlock()

	e.log.Info("equity fill", "symbol", sig.Instrument, "side", "buy",
		"price", fill.Price.String(), "qty", fill.Quantity.String(), "reason", sig.Reason)
}

func (e *Engine) sell(ctx context.Context, sig strategy.Signal) {
	e.mu.RLock()
	held := e.held[sig.Instrument]
	e.mu.RUnlock()
	if !held {
		return // nothing to close, mirrors the crypto engines' long-only rule
	}

	positions, err := e.trader.Positions(ctx)
	if err != nil {
		e.log.Warn("could not read positions; skipping sell", "symbol", sig.Instrument, "err", err)
		return
	}
	var qty model.Decimal
	for _, p := range positions {
		if strings.EqualFold(p.Symbol, string(sig.Instrument)) {
			qty = p.Qty
		}
	}
	if qty.IsZero() {
		e.mu.Lock()
		delete(e.held, sig.Instrument)
		e.mu.Unlock()
		return
	}

	fill, err := e.trader.PlaceEquityOrder(ctx, alpaca.EquityOrder{
		Symbol: string(sig.Instrument), Side: broker.Sell, Qty: qty,
		ClientID: clientOrderID(), Reason: sig.Reason,
	})
	if err != nil {
		e.log.Warn("equity order failed", "symbol", sig.Instrument, "side", "sell", "err", err)
		return
	}

	e.mu.Lock()
	e.held[sig.Instrument] = false
	e.mu.Unlock()

	e.log.Info("equity fill", "symbol", sig.Instrument, "side", "sell",
		"price", fill.Price.String(), "qty", fill.Quantity.String(), "reason", sig.Reason)
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

func clientOrderID() string {
	var b [12]byte
	_, _ = crand.Read(b[:])
	return "eq-" + hex.EncodeToString(b[:])
}

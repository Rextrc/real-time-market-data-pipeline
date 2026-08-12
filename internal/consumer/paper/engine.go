package paper

import (
	"context"
	"log/slog"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/Rextrc/real-time-market-data-pipeline/internal/bus"
	"github.com/Rextrc/real-time-market-data-pipeline/internal/consumer/candles"
	"github.com/Rextrc/real-time-market-data-pipeline/internal/model"
	"github.com/Rextrc/real-time-market-data-pipeline/internal/store"
	"github.com/Rextrc/real-time-market-data-pipeline/internal/strategy"
)

// EngineConfig tunes the trading loop.
type EngineConfig struct {
	// Name identifies this account in the API and in log lines. With
	// several strategies running side by side it is the only way to tell
	// their results apart.
	Name string
	// EvalInterval is how often the strategy is asked for signals.
	// Evaluating per tick would tie strategy cost to market volatility,
	// which loads the system hardest exactly when it is busiest.
	EvalInterval time.Duration
	// FillLatency delays execution after a signal, so fills use a price the
	// strategy did not see. Zero-latency fills are the most flattering and
	// least realistic assumption a paper engine can make.
	FillLatency time.Duration
	// MarkInterval is how often unrealized P&L and drawdown are recomputed.
	MarkInterval time.Duration
	// StatePath persists the book so a restart mid-experiment does not
	// discard the result. Empty disables persistence.
	StatePath string
	// SaveInterval is how often the book is written.
	SaveInterval time.Duration
	// History, if set, receives a periodic equity snapshot — this is what
	// lets a dashboard draw an equity curve instead of only ever showing
	// the current number. Nil disables it; the account still works, there
	// is just no history to chart.
	History store.EquityRecorder
	// HistoryInterval is how often a snapshot is recorded.
	HistoryInterval time.Duration
}

func DefaultEngineConfig() EngineConfig {
	return EngineConfig{
		EvalInterval:    15 * time.Second,
		FillLatency:     500 * time.Millisecond,
		MarkInterval:    5 * time.Second,
		SaveInterval:    30 * time.Second,
		HistoryInterval: 5 * time.Minute,
	}
}

// Engine runs a strategy against live prices into a paper account.
//
// It is also the Market implementation the strategy sees, which keeps the
// strategy's view read-only and free of any way to reach the account.
type Engine struct {
	cfg     EngineConfig
	acct    *Account
	strat   strategy.Strategy
	builder *candles.Builder
	venue   model.VenueID
	log     *slog.Logger

	mu    sync.RWMutex
	last  map[model.InstrumentID]model.Tick
	insts []model.InstrumentID
}

func NewEngine(
	cfg EngineConfig,
	acct *Account,
	strat strategy.Strategy,
	builder *candles.Builder,
	venue model.VenueID,
	instruments []model.InstrumentID,
	log *slog.Logger,
) *Engine {
	d := DefaultEngineConfig()
	if cfg.EvalInterval <= 0 {
		cfg.EvalInterval = d.EvalInterval
	}
	if cfg.MarkInterval <= 0 {
		cfg.MarkInterval = d.MarkInterval
	}
	if cfg.SaveInterval <= 0 {
		cfg.SaveInterval = d.SaveInterval
	}
	if cfg.HistoryInterval <= 0 {
		cfg.HistoryInterval = d.HistoryInterval
	}
	return &Engine{
		cfg: cfg, acct: acct, strat: strat, builder: builder,
		venue: venue, log: log,
		last:  make(map[model.InstrumentID]model.Tick),
		insts: instruments,
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

// -----------------------------------------------------------------------

// Name identifies this account.
func (e *Engine) Name() string {
	if e.cfg.Name == "" {
		return e.strat.Name()
	}
	return e.cfg.Name
}

// Account exposes the book for the API.
func (e *Engine) Account() *Account { return e.acct }

// StrategyName reports which strategy is running.
func (e *Engine) StrategyName() string { return e.strat.Name() }

// Run consumes ticks, evaluates the strategy on a timer, and applies fills.
func (e *Engine) Run(ctx context.Context, sub bus.Subscription) error {
	e.restore()

	evalTicker := time.NewTicker(e.cfg.EvalInterval)
	defer evalTicker.Stop()
	markTicker := time.NewTicker(e.cfg.MarkInterval)
	defer markTicker.Stop()
	saveTicker := time.NewTicker(e.cfg.SaveInterval)
	defer saveTicker.Stop()
	historyTicker := time.NewTicker(e.cfg.HistoryInterval)
	defer historyTicker.Stop()

	defer e.save()

	// Record one point at startup rather than waiting a full interval, so a
	// freshly (re)started engine has a curve immediately instead of a flat
	// line for the first several minutes.
	e.recordHistory(ctx)

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

		case <-markTicker.C:
			e.acct.Mark(e.prices())

		case <-saveTicker.C:
			e.save()

		case <-historyTicker.C:
			e.recordHistory(ctx)

		case <-evalTicker.C:
			e.evaluate(ctx)
		}
	}
}

func (e *Engine) evaluate(ctx context.Context) {
	sigs, err := e.strat.Evaluate(ctx, e)
	if err != nil {
		e.log.Warn("strategy evaluation failed", "strategy", e.strat.Name(), "err", err)
		return
	}

	for _, sig := range sigs {
		if sig.Action == strategy.ActionHold {
			continue
		}

		// Model execution latency: wait, then fill at whatever the price is
		// then, not at the price that produced the signal.
		if e.cfg.FillLatency > 0 {
			select {
			case <-time.After(e.cfg.FillLatency):
			case <-ctx.Done():
				return
			}
		}

		tick, ok := e.Last(sig.Instrument)
		if !ok {
			e.log.Warn("no price for signal", "instrument", sig.Instrument)
			continue
		}

		if _, err := e.acct.Apply(sig, tick.Price, time.Now().UTC()); err != nil {
			e.log.Warn("fill rejected", "instrument", sig.Instrument, "err", err)
		}
	}
}

func (e *Engine) prices() map[model.InstrumentID]model.Decimal {
	e.mu.RLock()
	defer e.mu.RUnlock()

	out := make(map[model.InstrumentID]model.Decimal, len(e.last))
	for id, t := range e.last {
		out[id] = t.Price
	}
	return out
}

// recordHistory writes one equity snapshot. Best-effort: a failed write here
// costs one point off a chart, not the run itself, so it logs and moves on
// rather than propagating an error that would tear down the engine.
func (e *Engine) recordHistory(ctx context.Context) {
	if e.cfg.History == nil {
		return
	}

	now := time.Now().UTC()
	snap := e.acct.Snapshot(now)

	writeCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
	defer cancel()

	err := e.cfg.History.RecordEquity(writeCtx, store.EquitySnapshot{
		Strategy:     e.Name(),
		Time:         now,
		Equity:       snap.Equity,
		RealizedPL:   snap.RealizedPL,
		UnrealizedPL: snap.UnrealizedPL,
		Trades:       snap.Trades,
	})
	if err != nil {
		e.log.Warn("equity history write failed", "err", err)
	}
}

func (e *Engine) save() {
	if e.cfg.StatePath == "" {
		return
	}

	b, err := e.acct.MarshalState()
	if err != nil {
		e.log.Error("paper state marshal failed", "err", err)
		return
	}

	// Write to a temp file and rename, so a crash mid-write cannot leave a
	// truncated book behind. Losing a week's experiment to a partial write
	// would be a miserable way to learn this.
	tmp := e.cfg.StatePath + ".tmp"
	if err := os.MkdirAll(filepath.Dir(e.cfg.StatePath), 0o755); err != nil {
		e.log.Error("paper state dir failed", "err", err)
		return
	}
	if err := os.WriteFile(tmp, b, 0o644); err != nil {
		e.log.Error("paper state write failed", "err", err)
		return
	}
	if err := os.Rename(tmp, e.cfg.StatePath); err != nil {
		e.log.Error("paper state rename failed", "err", err)
	}
}

func (e *Engine) restore() {
	if e.cfg.StatePath == "" {
		return
	}

	b, err := os.ReadFile(e.cfg.StatePath)
	if err != nil {
		if !os.IsNotExist(err) {
			e.log.Error("paper state read failed", "err", err)
		}
		return
	}
	if err := e.acct.UnmarshalState(b); err != nil {
		e.log.Error("paper state restore failed", "err", err)
		return
	}

	snap := e.acct.Snapshot(time.Now().UTC())
	e.log.Info("paper account restored",
		"equity", snap.Equity.String(), "trades", snap.Trades,
		"realized_pl", snap.RealizedPL.String())
}

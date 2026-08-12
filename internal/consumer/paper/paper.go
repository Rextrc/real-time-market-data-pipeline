// Package paper is a simulated trading account.
//
// It executes strategy signals against live prices without touching an
// exchange, so a strategy can be run for a week at zero financial risk and
// judged on its P&L afterwards.
//
// The simulation is deliberately pessimistic. A paper engine that fills
// instantly at the last trade price with no costs will make almost any
// strategy look profitable, and that flattery is the single most common
// reason a strategy that "worked" on paper loses money live. Three costs are
// modelled here, and all three are real:
//
//   - Fees, charged on both entry and exit.
//   - Slippage: you cross the spread, so you buy above and sell below the
//     last print.
//   - Latency: the fill uses the price after a configured delay, not the
//     price that triggered the signal.
//
// What is still NOT modelled, and would make live results worse: order book
// depth (a large order walks the book), partial fills, rejects, exchange
// downtime, and funding costs. Treat paper P&L as an optimistic bound.
package paper

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/Rextrc/real-time-market-data-pipeline/internal/model"
	"github.com/Rextrc/real-time-market-data-pipeline/internal/strategy"
)

// Config parameterizes the simulation.
type Config struct {
	// StartingCash is the initial balance in the quote currency (USDT).
	StartingCash model.Decimal
	// FeeRate is charged on the notional of every fill. 0.001 = 10bps,
	// which is roughly Binance's spot taker fee.
	FeeRate float64
	// SlippageRate is the fraction of price given up crossing the spread.
	// 0.0005 = 5bps.
	SlippageRate float64
	// MaxPositionFraction caps one position's notional as a fraction of
	// equity. This is the risk limit that stops a single runaway signal
	// from becoming the whole account.
	MaxPositionFraction float64
	// PricePrecision is the decimal scale used for recorded prices.
	PricePrecision uint8
	// QuantityPrecision is the decimal scale for recorded sizes.
	QuantityPrecision uint8
}

// DefaultConfig is a conservative starting point modelled on Binance spot
// taker fees.
func DefaultConfig() Config {
	return Config{
		StartingCash:        model.Decimal{Unscaled: 1000000, Scale: 2}, // 10,000.00
		FeeRate:             0.001,
		SlippageRate:        0.0005,
		MaxPositionFraction: 0.25,
		PricePrecision:      8,
		QuantityPrecision:   8,
	}
}

// Fill is one executed simulated trade.
type Fill struct {
	Time       time.Time          `json:"time"`
	Instrument model.InstrumentID `json:"instrument"`
	Side       strategy.Action    `json:"side"`
	Price      model.Decimal      `json:"price"`
	Quantity   model.Decimal      `json:"quantity"`
	Notional   model.Decimal      `json:"notional"`
	Fee        model.Decimal      `json:"fee"`
	// RealizedPL is the profit or loss this fill locked in. Non-zero only
	// on exits.
	RealizedPL model.Decimal `json:"realized_pl"`
	Reason     string        `json:"reason"`
}

// Position is an open holding.
type Position struct {
	Instrument model.InstrumentID `json:"instrument"`
	Quantity   model.Decimal      `json:"quantity"`
	// AvgEntry is the cost basis, fees included.
	AvgEntry model.Decimal `json:"avg_entry"`
	OpenedAt time.Time     `json:"opened_at"`
}

// Snapshot is the account at a point in time.
type Snapshot struct {
	Time           time.Time     `json:"time"`
	StartingCash   model.Decimal `json:"starting_cash"`
	Cash           model.Decimal `json:"cash"`
	PositionValue  model.Decimal `json:"position_value"`
	Equity         model.Decimal `json:"equity"`
	RealizedPL     model.Decimal `json:"realized_pl"`
	UnrealizedPL   model.Decimal `json:"unrealized_pl"`
	TotalPL        model.Decimal `json:"total_pl"`
	ReturnPct      float64       `json:"return_pct"`
	FeesPaid       model.Decimal `json:"fees_paid"`
	Trades         int           `json:"trades"`
	Wins           int           `json:"wins"`
	Losses         int           `json:"losses"`
	WinRatePct     float64       `json:"win_rate_pct"`
	MaxDrawdownPct float64       `json:"max_drawdown_pct"`
	Positions      []Position    `json:"positions"`
}

// Account is the simulated book.
type Account struct {
	cfg Config
	log *slog.Logger

	mu         sync.RWMutex
	cash       model.Decimal
	positions  map[model.InstrumentID]*Position
	lastPrices map[model.InstrumentID]model.Decimal
	fills      []Fill
	realized   model.Decimal
	fees       model.Decimal
	wins       int
	losses     int
	peakEquity model.Decimal
	maxDD      float64
}

func NewAccount(cfg Config, log *slog.Logger) *Account {
	if cfg.StartingCash.IsZero() {
		cfg.StartingCash = DefaultConfig().StartingCash
	}
	if cfg.PricePrecision == 0 {
		cfg.PricePrecision = 8
	}
	if cfg.QuantityPrecision == 0 {
		cfg.QuantityPrecision = 8
	}
	if cfg.MaxPositionFraction <= 0 {
		cfg.MaxPositionFraction = DefaultConfig().MaxPositionFraction
	}
	return &Account{
		cfg:        cfg,
		log:        log,
		cash:       cfg.StartingCash,
		positions:  make(map[model.InstrumentID]*Position),
		lastPrices: make(map[model.InstrumentID]model.Decimal),
		peakEquity: cfg.StartingCash,
	}
}

// Apply executes a signal at the given reference price.
//
// The caller supplies the price rather than the account fetching it, so that
// latency modelling — using the price as of signal time plus a delay — stays
// the caller's decision and remains testable.
func (a *Account) Apply(sig strategy.Signal, refPrice model.Decimal, now time.Time) (*Fill, error) {
	if refPrice.IsZero() || refPrice.IsNegative() {
		return nil, fmt.Errorf("paper: refusing to trade %s at price %s", sig.Instrument, refPrice)
	}

	a.mu.Lock()
	defer a.mu.Unlock()

	switch sig.Action {
	case strategy.ActionBuy:
		return a.buy(sig, refPrice, now)
	case strategy.ActionSell:
		return a.sell(sig, refPrice, now)
	default:
		return nil, nil
	}
}

func (a *Account) buy(sig strategy.Signal, refPrice model.Decimal, now time.Time) (*Fill, error) {
	if _, held := a.positions[sig.Instrument]; held {
		return nil, nil // already long; this engine is long-only and one position per instrument
	}

	// Buying crosses the spread upward.
	fillPrice := applyRate(refPrice, 1+a.cfg.SlippageRate, a.cfg.PricePrecision)

	equity := a.equityLocked()
	budget := model.FromFloat(equity.Float()*a.cfg.MaxPositionFraction*clamp01(sig.Strength), 2)
	if budget.Cmp(a.cash) > 0 {
		budget = a.cash
	}

	// Reserve headroom for the entry fee. Without this, a full-size order
	// (MaxPositionFraction of 1.0) computes a notional equal to the entire
	// cash balance, and then cannot afford its own fee — so the order is
	// silently rejected and the account never trades at all.
	budget = model.FromFloat(budget.Float()/(1+a.cfg.FeeRate), 2)
	if budget.Float() <= 0 {
		return nil, nil
	}

	qty := model.FromFloat(budget.Float()/fillPrice.Float(), a.cfg.QuantityPrecision)
	if qty.Float() <= 0 {
		return nil, nil
	}

	notional := fillPrice.Mul(qty).Rescale(2)
	fee := model.FromFloat(notional.Float()*a.cfg.FeeRate, 2)
	total := notional.Add(fee)

	if total.Cmp(a.cash) > 0 {
		return nil, nil // cannot afford it after fees
	}

	a.cash = a.cash.Sub(total)
	a.fees = a.fees.Add(fee)

	// Cost basis includes the fee, so a position must clear its entry cost
	// before it counts as profitable.
	basis := model.FromFloat(total.Float()/qty.Float(), a.cfg.PricePrecision)
	a.positions[sig.Instrument] = &Position{
		Instrument: sig.Instrument,
		Quantity:   qty,
		AvgEntry:   basis,
		OpenedAt:   now,
	}

	f := Fill{
		Time: now, Instrument: sig.Instrument, Side: strategy.ActionBuy,
		Price: fillPrice, Quantity: qty, Notional: notional, Fee: fee,
		Reason: sig.Reason,
	}
	a.record(f)
	return &f, nil
}

func (a *Account) sell(sig strategy.Signal, refPrice model.Decimal, now time.Time) (*Fill, error) {
	pos, held := a.positions[sig.Instrument]
	if !held {
		return nil, nil // long-only: nothing to close, and no shorting
	}

	// Selling crosses the spread downward.
	fillPrice := applyRate(refPrice, 1-a.cfg.SlippageRate, a.cfg.PricePrecision)

	notional := fillPrice.Mul(pos.Quantity).Rescale(2)
	fee := model.FromFloat(notional.Float()*a.cfg.FeeRate, 2)
	proceeds := notional.Sub(fee)

	cost := pos.AvgEntry.Mul(pos.Quantity).Rescale(2)
	realized := proceeds.Sub(cost)

	a.cash = a.cash.Add(proceeds)
	a.fees = a.fees.Add(fee)
	a.realized = a.realized.Add(realized)
	if realized.IsNegative() {
		a.losses++
	} else {
		a.wins++
	}
	delete(a.positions, sig.Instrument)

	f := Fill{
		Time: now, Instrument: sig.Instrument, Side: strategy.ActionSell,
		Price: fillPrice, Quantity: pos.Quantity, Notional: notional, Fee: fee,
		RealizedPL: realized, Reason: sig.Reason,
	}
	a.record(f)
	return &f, nil
}

func (a *Account) record(f Fill) {
	a.fills = append(a.fills, f)
	a.log.Info("paper fill",
		"side", f.Side, "instrument", f.Instrument,
		"price", f.Price.String(), "qty", f.Quantity.String(),
		"notional", f.Notional.String(), "fee", f.Fee.String(),
		"realized_pl", f.RealizedPL.String(), "reason", f.Reason)
}

// Mark updates unrealized P&L and the drawdown watermark against current
// prices. Call it on a timer; it is what makes equity meaningful between
// trades.
func (a *Account) Mark(prices map[model.InstrumentID]model.Decimal) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.lastPrices = prices

	equity := a.equityLocked()
	if equity.Cmp(a.peakEquity) > 0 {
		a.peakEquity = equity
	}
	if peak := a.peakEquity.Float(); peak > 0 {
		if dd := (peak - equity.Float()) / peak * 100; dd > a.maxDD {
			a.maxDD = dd
		}
	}
}

// Snapshot reports the account state.
func (a *Account) Snapshot(now time.Time) Snapshot {
	a.mu.RLock()
	defer a.mu.RUnlock()

	positionValue := a.positionValueLocked()
	equity := a.cash.Add(positionValue)

	var unrealized model.Decimal
	for id, p := range a.positions {
		if px, ok := a.lastPrices[id]; ok {
			unrealized = unrealized.Add(px.Sub(p.AvgEntry).Mul(p.Quantity).Rescale(2))
		}
	}

	total := equity.Sub(a.cfg.StartingCash)
	var returnPct float64
	if start := a.cfg.StartingCash.Float(); start > 0 {
		returnPct = total.Float() / start * 100
	}

	positions := make([]Position, 0, len(a.positions))
	for _, p := range a.positions {
		positions = append(positions, *p)
	}

	var winRate float64
	if closed := a.wins + a.losses; closed > 0 {
		winRate = float64(a.wins) / float64(closed) * 100
	}

	return Snapshot{
		Time:           now,
		StartingCash:   a.cfg.StartingCash,
		Cash:           a.cash.Rescale(2),
		PositionValue:  positionValue,
		Equity:         equity.Rescale(2),
		RealizedPL:     a.realized.Rescale(2),
		UnrealizedPL:   unrealized,
		TotalPL:        total.Rescale(2),
		ReturnPct:      returnPct,
		FeesPaid:       a.fees.Rescale(2),
		Trades:         len(a.fills),
		Wins:           a.wins,
		Losses:         a.losses,
		WinRatePct:     winRate,
		MaxDrawdownPct: a.maxDD,
		Positions:      positions,
	}
}

// Fills returns the trade log, most recent last.
func (a *Account) Fills(limit int) []Fill {
	a.mu.RLock()
	defer a.mu.RUnlock()

	if limit <= 0 || limit > len(a.fills) {
		limit = len(a.fills)
	}
	out := make([]Fill, limit)
	copy(out, a.fills[len(a.fills)-limit:])
	return out
}

func (a *Account) equityLocked() model.Decimal {
	return a.cash.Add(a.positionValueLocked())
}

func (a *Account) positionValueLocked() model.Decimal {
	var total model.Decimal
	for id, p := range a.positions {
		px, ok := a.lastPrices[id]
		if !ok {
			px = p.AvgEntry // no mark yet: value at cost rather than at zero
		}
		total = total.Add(px.Mul(p.Quantity).Rescale(2))
	}
	return total.Rescale(2)
}

// Snapshot persistence lets a week-long run survive a restart. Losing the
// book to a deploy would make the whole experiment worthless.
type persisted struct {
	Cash      model.Decimal                    `json:"cash"`
	Positions map[model.InstrumentID]*Position `json:"positions"`
	Fills     []Fill                           `json:"fills"`
	Realized  model.Decimal                    `json:"realized"`
	Fees      model.Decimal                    `json:"fees"`
	Wins      int                              `json:"wins"`
	Losses    int                              `json:"losses"`
	Peak      model.Decimal                    `json:"peak_equity"`
	MaxDD     float64                          `json:"max_drawdown_pct"`
}

// MarshalState serializes the book.
func (a *Account) MarshalState() ([]byte, error) {
	a.mu.RLock()
	defer a.mu.RUnlock()

	return json.Marshal(persisted{
		Cash: a.cash, Positions: a.positions, Fills: a.fills,
		Realized: a.realized, Fees: a.fees,
		Wins: a.wins, Losses: a.losses,
		Peak: a.peakEquity, MaxDD: a.maxDD,
	})
}

// UnmarshalState restores a book saved by MarshalState.
func (a *Account) UnmarshalState(b []byte) error {
	var p persisted
	if err := json.Unmarshal(b, &p); err != nil {
		return fmt.Errorf("paper: restore: %w", err)
	}

	a.mu.Lock()
	defer a.mu.Unlock()

	a.cash = p.Cash
	a.positions = p.Positions
	if a.positions == nil {
		a.positions = make(map[model.InstrumentID]*Position)
	}
	a.fills = p.Fills
	a.realized = p.Realized
	a.fees = p.Fees
	a.wins, a.losses = p.Wins, p.Losses
	a.peakEquity = p.Peak
	a.maxDD = p.MaxDD
	return nil
}

func applyRate(d model.Decimal, rate float64, scale uint8) model.Decimal {
	return model.FromFloat(d.Float()*rate, scale)
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

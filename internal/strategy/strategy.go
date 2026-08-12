// Package strategy defines the decision layer.
//
// A strategy sees market state and emits intents. It never talks to an
// exchange, never knows about fills, fees, or position sizing, and holds no
// money. That separation is what lets the same strategy run against live
// ticks, a replay of last Tuesday, or a paper account, with no changes.
package strategy

import (
	"context"
	"time"

	"github.com/Rextrc/real-time-market-data-pipeline/internal/consumer/candles"
	"github.com/Rextrc/real-time-market-data-pipeline/internal/model"
)

// Action is what a strategy wants to do.
type Action string

const (
	ActionHold Action = "hold"
	ActionBuy  Action = "buy"
	ActionSell Action = "sell"
)

// Signal is one decision about one instrument.
type Signal struct {
	Instrument model.InstrumentID `json:"instrument"`
	Action     Action             `json:"action"`
	// Strength is 0..1 and scales the position size. A strategy that has no
	// opinion about conviction should emit 1.
	Strength float64 `json:"strength"`
	// Reason is free text for the trade log. It is what makes a week-long
	// run auditable after the fact instead of a mystery.
	Reason string    `json:"reason"`
	Time   time.Time `json:"time"`
}

// Market is the read-only view a strategy gets.
type Market interface {
	// Last returns the most recent trade for an instrument.
	Last(id model.InstrumentID) (model.Tick, bool)
	// Candles returns recent bars, oldest first, including the in-progress
	// bar as the final element.
	Candles(id model.InstrumentID, limit int) []candles.Candle
	// Instruments lists what is being tracked.
	Instruments() []model.InstrumentID
}

// Strategy turns market state into signals.
//
// Evaluate is called on a schedule, not on every tick. Tick-rate evaluation
// would couple strategy cost to market volatility — precisely when the
// system is already under the most load.
type Strategy interface {
	Name() string
	Evaluate(ctx context.Context, m Market) ([]Signal, error)
}

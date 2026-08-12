package strategy

import (
	"context"
	"fmt"
	"time"

	"github.com/Rextrc/real-time-market-data-pipeline/internal/consumer/candles"
)

// Momentum is a moving-average crossover: go long when the fast average
// crosses above the slow one, flat when it crosses back below.
//
// It is deliberately the simplest strategy that is not a coin flip, and it
// is here as a baseline rather than as an edge. Crossover systems are the
// most-published trading rule in existence, which is a good reason to expect
// no free money in it: on liquid crypto pairs, after fees, it historically
// bleeds slowly in ranging markets and makes it back only in sustained
// trends. Treat its P&L as the number any other strategy has to beat.
type Momentum struct {
	Fast int
	Slow int
	// MinBars guards against trading on a handful of bars right after
	// startup, when the averages are meaningless.
	MinBars int
}

func NewMomentum(fast, slow int) *Momentum {
	if fast <= 0 {
		fast = 9
	}
	if slow <= fast {
		slow = fast * 3
	}
	return &Momentum{Fast: fast, Slow: slow, MinBars: slow + 2}
}

func (m *Momentum) Name() string { return fmt.Sprintf("momentum(%d/%d)", m.Fast, m.Slow) }

func (m *Momentum) Evaluate(ctx context.Context, mk Market) ([]Signal, error) {
	now := time.Now().UTC()
	var out []Signal

	for _, id := range mk.Instruments() {
		bars := mk.Candles(id, m.Slow+5)
		if len(bars) < m.MinBars {
			continue
		}

		// Exclude the in-progress bar. Acting on a partial bar means acting
		// on a number that will still change, which turns one decision into
		// a stream of contradictory ones and is the most common way a
		// backtest quietly diverges from live behavior.
		closed := bars[:len(bars)-1]
		if len(closed) < m.Slow+1 {
			continue
		}

		fastNow := sma(closed, m.Fast, 0)
		slowNow := sma(closed, m.Slow, 0)
		fastPrev := sma(closed, m.Fast, 1)
		slowPrev := sma(closed, m.Slow, 1)

		crossedUp := fastPrev <= slowPrev && fastNow > slowNow
		crossedDown := fastPrev >= slowPrev && fastNow < slowNow

		switch {
		case crossedUp:
			out = append(out, Signal{
				Instrument: id, Action: ActionBuy, Strength: 1, Time: now,
				Reason: fmt.Sprintf("fast SMA(%d)=%.2f crossed above slow SMA(%d)=%.2f",
					m.Fast, fastNow, m.Slow, slowNow),
			})
		case crossedDown:
			out = append(out, Signal{
				Instrument: id, Action: ActionSell, Strength: 1, Time: now,
				Reason: fmt.Sprintf("fast SMA(%d)=%.2f crossed below slow SMA(%d)=%.2f",
					m.Fast, fastNow, m.Slow, slowNow),
			})
		}
	}
	return out, nil
}

// sma averages the closes of n bars ending `back` bars from the end.
//
// This returns float64 rather than a Decimal on purpose: it is a statistic
// used to compare against another statistic, never a quantity that gets
// stored or settled. Money stays exact; indicators do not need to be.
func sma(bars []candles.Candle, n, back int) float64 {
	end := len(bars) - back
	if end < n || n <= 0 {
		return 0
	}
	var sum float64
	for _, b := range bars[end-n : end] {
		sum += b.Close.Float()
	}
	return sum / float64(n)
}

var _ Strategy = (*Momentum)(nil)

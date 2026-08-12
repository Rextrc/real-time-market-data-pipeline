package strategy

import (
	"context"
	"fmt"
	"math"
	"time"

	"github.com/Rextrc/real-time-market-data-pipeline/internal/consumer/candles"
)

// MeanReversion buys when price falls far enough below its recent average and
// sells when it returns to it.
//
// It exists as a deliberate counterweight to Momentum. The two are close to
// opposites: momentum buys strength and mean reversion buys weakness, so on
// the same data they should disagree most of the time. That makes the pair
// informative in a way one strategy alone is not — if both make money over a
// month, the market trended and chopped in turn; if both lose, you are
// watching fees; if they mirror each other, you are watching noise.
//
// Like the momentum baseline, this is a textbook rule with no expectation of
// edge. Its job is to be a second number.
type MeanReversion struct {
	// Window is how many bars the average and deviation are computed over.
	Window int
	// EntryZ is how many standard deviations below the mean triggers a buy.
	EntryZ float64
	// ExitZ is the level at which the position is closed. Exiting at the
	// mean rather than at a profit target is what makes this mean reversion
	// rather than a bet on direction.
	ExitZ float64
}

func NewMeanReversion(window int, entryZ, exitZ float64) *MeanReversion {
	if window < 5 {
		window = 30
	}
	if entryZ <= 0 {
		entryZ = 2.0
	}
	if exitZ >= entryZ {
		exitZ = 0.5
	}
	return &MeanReversion{Window: window, EntryZ: entryZ, ExitZ: exitZ}
}

func (m *MeanReversion) Name() string {
	return fmt.Sprintf("meanrev(%d/%.1fσ)", m.Window, m.EntryZ)
}

func (m *MeanReversion) Evaluate(ctx context.Context, mk Market) ([]Signal, error) {
	now := time.Now().UTC()
	var out []Signal

	for _, id := range mk.Instruments() {
		bars := mk.Candles(id, m.Window+5)
		if len(bars) < m.Window+2 {
			continue
		}

		// Exclude the in-progress bar: acting on a number that will still
		// change turns one decision into a stream of contradictory ones.
		closed := bars[:len(bars)-1]
		if len(closed) < m.Window {
			continue
		}
		window := closed[len(closed)-m.Window:]

		mean, sd := meanStdDev(window)
		if sd == 0 {
			continue // a flat window has no meaningful deviation
		}

		last := window[len(window)-1].Close.Float()
		z := (last - mean) / sd

		switch {
		case z <= -m.EntryZ:
			out = append(out, Signal{
				Instrument: id, Action: ActionBuy, Strength: 1, Time: now,
				Reason: fmt.Sprintf("price %.2f is %.1fσ below the %d-bar mean %.2f",
					last, -z, m.Window, mean),
			})
		case z >= -m.ExitZ:
			// Reverting to (or above) the mean closes the position. The
			// engine ignores a sell with no position open, so emitting this
			// unconditionally is safe and keeps the rule simple.
			out = append(out, Signal{
				Instrument: id, Action: ActionSell, Strength: 1, Time: now,
				Reason: fmt.Sprintf("price %.2f reverted to %.1fσ of the %d-bar mean %.2f",
					last, z, m.Window, mean),
			})
		}
	}
	return out, nil
}

// meanStdDev returns the sample mean and population standard deviation of the
// bars' closes. Float is correct here: these are statistics compared against
// each other, never money that gets stored or settled.
func meanStdDev(bars []candles.Candle) (mean, sd float64) {
	if len(bars) == 0 {
		return 0, 0
	}

	for _, b := range bars {
		mean += b.Close.Float()
	}
	mean /= float64(len(bars))

	var sumSq float64
	for _, b := range bars {
		d := b.Close.Float() - mean
		sumSq += d * d
	}
	return mean, math.Sqrt(sumSq / float64(len(bars)))
}

var _ Strategy = (*MeanReversion)(nil)

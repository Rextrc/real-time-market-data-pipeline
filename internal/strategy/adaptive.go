package strategy

import (
	"context"
	"fmt"
	"math/rand"
	"sync"
	"time"

	"github.com/Rextrc/real-time-market-data-pipeline/internal/consumer/candles"
	"github.com/Rextrc/real-time-market-data-pipeline/internal/model"
)

// AdaptiveArm is one candidate moving-average crossover the bandit can pick.
type AdaptiveArm struct {
	Fast, Slow int
}

// Adaptive runs several SMA-crossover configurations ("arms") against the
// same instrument and steers which one actually gets to trade using an
// epsilon-greedy bandit: the arm with the best recent realized return wins
// most evaluations, a random eligible arm wins occasionally so one that has
// gone quiet still gets re-tested instead of being abandoned forever.
//
// Using several short-lookback arms rather than one long one is also what
// makes this trade more often than Momentum — a 2/6 crossover on 1-minute
// bars fires many times a day where a 9/27 crossover fires a few times a
// week.
//
// This is a bandit chasing recent winners, not a forecaster: it adapts to a
// regime after it has already happened, and a short streak of luck can look
// identical to real edge over a handful of trades. Treat it the same as
// Momentum and MeanReversion — a baseline to compare against, not something
// to trust because the name says "adaptive."
type Adaptive struct {
	Arms []AdaptiveArm
	// Epsilon is the probability of exploring a random eligible arm instead
	// of exploiting the current best.
	Epsilon float64
	// Decay weights each arm's score toward its trade history: the score is
	// an EWMA of realized return with this much weight on the running score,
	// (1-Decay) on the newest trade. Closer to 1 remembers longer.
	Decay float64

	mu     sync.Mutex
	rng    *rand.Rand
	score  map[int]float64
	trades map[int]int
	open   map[model.InstrumentID]*adaptivePos
}

type adaptivePos struct {
	arm   int
	entry float64
}

// NewAdaptive builds the bandit. A nil arms list uses a spread from fast/
// noisy to slower/steadier, so the bandit has something to actually choose
// between rather than five near-duplicates of the same speed.
func NewAdaptive(arms []AdaptiveArm, epsilon, decay float64) *Adaptive {
	if len(arms) == 0 {
		arms = []AdaptiveArm{{2, 6}, {3, 9}, {5, 15}, {8, 21}, {13, 34}}
	}
	if epsilon <= 0 {
		epsilon = 0.15
	}
	if decay <= 0 || decay >= 1 {
		decay = 0.7
	}
	return &Adaptive{
		Arms: arms, Epsilon: epsilon, Decay: decay,
		rng:    rand.New(rand.NewSource(time.Now().UnixNano())),
		score:  make(map[int]float64),
		trades: make(map[int]int),
		open:   make(map[model.InstrumentID]*adaptivePos),
	}
}

func (a *Adaptive) Name() string {
	return fmt.Sprintf("adaptive(%d arms, eps=%.2f)", len(a.Arms), a.Epsilon)
}

func (a *Adaptive) maxSlow() int {
	m := 0
	for _, arm := range a.Arms {
		if arm.Slow > m {
			m = arm.Slow
		}
	}
	return m
}

func (a *Adaptive) Evaluate(ctx context.Context, mk Market) ([]Signal, error) {
	now := time.Now().UTC()
	var out []Signal
	need := a.maxSlow() + 2

	for _, id := range mk.Instruments() {
		bars := mk.Candles(id, a.maxSlow()+5)
		if len(bars) < need {
			continue
		}
		// Exclude the in-progress bar, same reasoning as Momentum: acting on
		// a number that will still change turns one decision into a stream
		// of contradictory ones.
		closed := bars[:len(bars)-1]
		if len(closed) < a.maxSlow()+1 {
			continue
		}
		last := closed[len(closed)-1].Close.Float()

		a.mu.Lock()
		pos, inPosition := a.open[id]
		a.mu.Unlock()

		if inPosition {
			sig, ok := a.evaluateExit(id, pos, closed, last, now)
			if ok {
				out = append(out, sig)
			}
			continue
		}

		sig, ok := a.evaluateEntry(id, closed, last, now)
		if ok {
			out = append(out, sig)
		}
	}
	return out, nil
}

// evaluateExit closes a position only on its own arm's exit signal — letting
// a different arm's crossover close it would credit or blame the wrong rule
// for the trade's outcome and corrupt what the bandit learns.
func (a *Adaptive) evaluateExit(id model.InstrumentID, pos *adaptivePos, closed []candles.Candle, last float64, now time.Time) (Signal, bool) {
	arm := a.Arms[pos.arm]
	if len(closed) < arm.Slow+1 {
		return Signal{}, false
	}
	if !crossedDown(closed, arm.Fast, arm.Slow) {
		return Signal{}, false
	}

	ret := (last - pos.entry) / pos.entry

	a.mu.Lock()
	a.score[pos.arm] = a.Decay*a.score[pos.arm] + (1-a.Decay)*ret
	a.trades[pos.arm]++
	score := a.score[pos.arm]
	trades := a.trades[pos.arm]
	delete(a.open, id)
	a.mu.Unlock()

	return Signal{
		Instrument: id, Action: ActionSell, Strength: 1, Time: now,
		Reason: fmt.Sprintf("arm %d/%d exit: %.2f -> %.2f (%+.2f%%), arm score now %+.3f%% over %d trades",
			arm.Fast, arm.Slow, pos.entry, last, ret*100, score*100, trades),
	}, true
}

// evaluateEntry finds every arm currently signaling a buy and picks one via
// the epsilon-greedy bandit.
func (a *Adaptive) evaluateEntry(id model.InstrumentID, closed []candles.Candle, last float64, now time.Time) (Signal, bool) {
	var eligible []int
	for i, arm := range a.Arms {
		if len(closed) < arm.Slow+1 {
			continue
		}
		if crossedUp(closed, arm.Fast, arm.Slow) {
			eligible = append(eligible, i)
		}
	}
	if len(eligible) == 0 {
		return Signal{}, false
	}

	a.mu.Lock()
	chosen := a.pickArm(eligible)
	a.open[id] = &adaptivePos{arm: chosen, entry: last}
	score := a.score[chosen]
	trades := a.trades[chosen]
	a.mu.Unlock()

	arm := a.Arms[chosen]
	return Signal{
		Instrument: id, Action: ActionBuy, Strength: 1, Time: now,
		Reason: fmt.Sprintf("arm %d/%d entry at %.2f (score %+.3f%% over %d trades; %d/%d arms eligible)",
			arm.Fast, arm.Slow, last, score*100, trades, len(eligible), len(a.Arms)),
	}, true
}

// pickArm is epsilon-greedy: explore a random eligible arm with probability
// Epsilon, otherwise exploit whichever eligible arm currently scores best.
// An arm with no trades yet scores exactly 0, which is optimistic enough to
// get tried before the bandit has any history to trust.
//
// Caller holds a.mu.
func (a *Adaptive) pickArm(eligible []int) int {
	if a.rng.Float64() < a.Epsilon {
		return eligible[a.rng.Intn(len(eligible))]
	}
	best := eligible[0]
	for _, i := range eligible[1:] {
		if a.score[i] > a.score[best] {
			best = i
		}
	}
	return best
}

func crossedUp(bars []candles.Candle, fast, slow int) bool {
	fastNow, slowNow := sma(bars, fast, 0), sma(bars, slow, 0)
	fastPrev, slowPrev := sma(bars, fast, 1), sma(bars, slow, 1)
	return fastPrev <= slowPrev && fastNow > slowNow
}

func crossedDown(bars []candles.Candle, fast, slow int) bool {
	fastNow, slowNow := sma(bars, fast, 0), sma(bars, slow, 0)
	fastPrev, slowPrev := sma(bars, fast, 1), sma(bars, slow, 1)
	return fastPrev >= slowPrev && fastNow < slowNow
}

var _ Strategy = (*Adaptive)(nil)

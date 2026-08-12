// Package candles aggregates ticks into OHLCV bars.
//
// This is the moderate-speed consumer: more work per tick than the
// persister, far less than a strategy backtest. It is also the thing most
// downstream analysis actually reads, so it keeps a bounded in-memory
// history that the API can serve without touching disk.
package candles

import (
	"context"
	"sync"
	"time"

	"github.com/Rextrc/real-time-market-data-pipeline/internal/bus"
	"github.com/Rextrc/real-time-market-data-pipeline/internal/model"
)

// Candle is one OHLCV bar.
//
// Prices stay exact fixed-point. Volume is accumulated as a scaled integer
// at the maximum scale seen, so summing thousands of quantities introduces
// no drift — which a float accumulator absolutely would over a trading day.
type Candle struct {
	Instrument model.InstrumentID `json:"instrument"`
	Venue      model.VenueID      `json:"venue"`
	OpenTime   time.Time          `json:"open_time"`
	CloseTime  time.Time          `json:"close_time"`
	Open       model.Decimal      `json:"open"`
	High       model.Decimal      `json:"high"`
	Low        model.Decimal      `json:"low"`
	Close      model.Decimal      `json:"close"`
	Volume     model.Decimal      `json:"volume"`
	Trades     int                `json:"trades"`
	Complete   bool               `json:"complete"`
}

// Bucket returns the bar's start time for an interval.
func Bucket(t time.Time, interval time.Duration) time.Time {
	return t.UTC().Truncate(interval)
}

// Builder aggregates one interval across many instruments.
type Builder struct {
	interval time.Duration
	history  int

	mu   sync.RWMutex
	open map[key]*Candle
	done map[key][]Candle
}

type key struct {
	venue      model.VenueID
	instrument model.InstrumentID
}

// New builds candles of the given interval, retaining `history` completed
// bars per instrument in memory.
func New(interval time.Duration, history int) *Builder {
	if interval <= 0 {
		interval = time.Minute
	}
	if history <= 0 {
		history = 500
	}
	return &Builder{
		interval: interval,
		history:  history,
		open:     make(map[key]*Candle),
		done:     make(map[key][]Candle),
	}
}

func (b *Builder) Interval() time.Duration { return b.interval }

// Add folds one tick into the current bar. Exported so a backfill can drive
// the same code path as the live consumer.
func (b *Builder) Add(t model.Tick) {
	k := key{t.Venue, t.Instrument}
	bucket := Bucket(t.EventTime, b.interval)

	b.mu.Lock()
	defer b.mu.Unlock()

	cur, ok := b.open[k]
	switch {
	case !ok:
		b.open[k] = b.start(t, bucket)
		return

	case bucket.After(cur.OpenTime):
		cur.Complete = true
		b.retain(k, *cur)
		b.open[k] = b.start(t, bucket)
		return

	case bucket.Before(cur.OpenTime):
		// A late tick for a bar already closed. Dropping it is deliberate:
		// mutating a published bar would make the same query return
		// different answers over time. It is counted nowhere yet, which is
		// worth revisiting if late data turns out to be common.
		return
	}

	// Same bucket: fold in.
	if cur.High.Less(t.Price) {
		cur.High = t.Price
	}
	if t.Price.Less(cur.Low) {
		cur.Low = t.Price
	}
	cur.Close = t.Price
	cur.CloseTime = t.EventTime
	cur.Volume = cur.Volume.Add(t.Quantity)
	cur.Trades++
}

func (b *Builder) start(t model.Tick, bucket time.Time) *Candle {
	return &Candle{
		Instrument: t.Instrument,
		Venue:      t.Venue,
		OpenTime:   bucket,
		CloseTime:  t.EventTime,
		Open:       t.Price,
		High:       t.Price,
		Low:        t.Price,
		Close:      t.Price,
		Volume:     t.Quantity,
		Trades:     1,
	}
}

func (b *Builder) retain(k key, c Candle) {
	h := append(b.done[k], c)
	if len(h) > b.history {
		// Copy rather than reslice so the backing array can be collected;
		// otherwise a long-running process holds every candle it ever made.
		h = append([]Candle(nil), h[len(h)-b.history:]...)
	}
	b.done[k] = h
}

// Recent returns completed bars for an instrument, oldest first, plus the
// in-progress bar if there is one.
func (b *Builder) Recent(venue model.VenueID, id model.InstrumentID, limit int) []Candle {
	k := key{venue, id}

	b.mu.RLock()
	defer b.mu.RUnlock()

	hist := b.done[k]
	if limit > 0 && len(hist) > limit {
		hist = hist[len(hist)-limit:]
	}

	out := make([]Candle, 0, len(hist)+1)
	out = append(out, hist...)
	if cur, ok := b.open[k]; ok {
		out = append(out, *cur)
	}
	return out
}

// Current returns the in-progress bar.
func (b *Builder) Current(venue model.VenueID, id model.InstrumentID) (Candle, bool) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	c, ok := b.open[key{venue, id}]
	if !ok {
		return Candle{}, false
	}
	return *c, true
}

// Run drives the builder from a subscription.
func (b *Builder) Run(ctx context.Context, sub bus.Subscription) error {
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-sub.Done():
			return nil
		case t := <-sub.Ticks():
			b.Add(t)
		}
	}
}

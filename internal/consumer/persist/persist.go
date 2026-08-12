// Package persist writes ticks to durable storage.
//
// This is the lossless consumer: it must not drop. Its subscription
// therefore uses PolicyBlock only when fed by a producer we control, and
// otherwise relies on a deep queue plus alerting on depth — because the one
// thing worse than a slow archive is a silently incomplete one.
package persist

import (
	"context"
	"log/slog"
	"sync/atomic"
	"time"

	"github.com/Rextrc/real-time-market-data-pipeline/internal/bus"
	"github.com/Rextrc/real-time-market-data-pipeline/internal/model"
	"github.com/Rextrc/real-time-market-data-pipeline/internal/store"
)

// Config tunes the batching behavior.
type Config struct {
	// BatchSize flushes once this many ticks are buffered.
	BatchSize int
	// FlushInterval flushes a partial batch after this long, so a quiet
	// instrument's last trade is not stranded in memory indefinitely.
	FlushInterval time.Duration
}

// DefaultConfig batches aggressively enough to keep the fsync cost amortized
// while bounding worst-case data loss to FlushInterval.
func DefaultConfig() Config {
	return Config{BatchSize: 500, FlushInterval: 250 * time.Millisecond}
}

// Stats reports what the persister has done.
type Stats struct {
	Received  uint64 `json:"received"`
	Stored    uint64 `json:"stored"`
	Duplicate uint64 `json:"duplicate"`
	Flushes   uint64 `json:"flushes"`
	Errors    uint64 `json:"errors"`
}

// Persister drains a subscription into a store.
type Persister struct {
	sub   bus.Subscription
	store store.Appender
	cfg   Config
	log   *slog.Logger

	received  atomic.Uint64
	stored    atomic.Uint64
	duplicate atomic.Uint64
	flushes   atomic.Uint64
	errors    atomic.Uint64
}

func New(sub bus.Subscription, st store.Appender, cfg Config, log *slog.Logger) *Persister {
	if cfg.BatchSize <= 0 {
		cfg.BatchSize = DefaultConfig().BatchSize
	}
	if cfg.FlushInterval <= 0 {
		cfg.FlushInterval = DefaultConfig().FlushInterval
	}
	return &Persister{sub: sub, store: st, cfg: cfg, log: log}
}

func (p *Persister) Stats() Stats {
	return Stats{
		Received:  p.received.Load(),
		Stored:    p.stored.Load(),
		Duplicate: p.duplicate.Load(),
		Flushes:   p.flushes.Load(),
		Errors:    p.errors.Load(),
	}
}

// Run drains until the context is cancelled or the subscription ends, then
// flushes what it is holding.
func (p *Persister) Run(ctx context.Context) error {
	batch := make([]model.Tick, 0, p.cfg.BatchSize)
	ticker := time.NewTicker(p.cfg.FlushInterval)
	defer ticker.Stop()

	flush := func(reason string) {
		if len(batch) == 0 {
			return
		}
		// Deliberately not ctx: a cancelled context must still be able to
		// write the final batch, or every clean shutdown would silently
		// discard up to BatchSize ticks.
		writeCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
		defer cancel()

		stored, err := p.store.Append(writeCtx, batch)
		p.flushes.Add(1)
		if err != nil {
			p.errors.Add(1)
			p.log.Error("persist flush failed", "reason", reason, "ticks", len(batch), "err", err)
		} else {
			p.stored.Add(uint64(stored))
			p.duplicate.Add(uint64(len(batch) - stored))
		}
		batch = batch[:0]
	}

	for {
		select {
		case <-ctx.Done():
			flush("shutdown")
			return nil

		case <-p.sub.Done():
			flush("unsubscribed")
			return nil

		case t := <-p.sub.Ticks():
			p.received.Add(1)
			batch = append(batch, t)
			if len(batch) >= p.cfg.BatchSize {
				flush("full")
			}

		case <-ticker.C:
			flush("interval")
		}
	}
}

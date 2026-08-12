package store

import (
	"context"
	"log/slog"
	"time"
)

// Pruner is the retention half of a store.
type Pruner interface {
	Prune(ctx context.Context, ticksBefore, rawBefore time.Time) (deletedTicks, deletedRaw int64, err error)
	PruneToSize(ctx context.Context, maxBytes int64) (deletedTicks int64, err error)
	SizeBytes(ctx context.Context) (int64, error)
}

// RetentionConfig bounds how much history is kept.
//
// On a fixed-size volume this is not a nicety. Ingest writes continuously,
// and an unbounded archive means the run ends when the disk fills — usually
// at 3am, usually with writes failing silently behind a queue that is
// dropping. Deciding up front what to throw away is how a month-long run
// finishes.
type RetentionConfig struct {
	// Ticks older than this are deleted. Zero keeps them forever.
	Ticks time.Duration
	// Raw frames older than this are deleted. Zero keeps them forever.
	// This is the big one — raw frames are roughly two thirds of the bytes.
	Raw time.Duration
	// MaxBytes is a hard ceiling. When the database exceeds it, whole days
	// of ticks are dropped oldest-first until it fits. This is the last
	// resort that keeps the process alive when the time-based settings turn
	// out to have been too generous.
	MaxBytes int64
	// Interval between retention passes.
	Interval time.Duration
}

// Janitor enforces a RetentionConfig on a schedule.
type Janitor struct {
	store Pruner
	cfg   RetentionConfig
	log   *slog.Logger
}

func NewJanitor(s Pruner, cfg RetentionConfig, log *slog.Logger) *Janitor {
	if cfg.Interval <= 0 {
		cfg.Interval = time.Hour
	}
	return &Janitor{store: s, cfg: cfg, log: log}
}

// Run prunes on the configured interval until the context is cancelled.
func (j *Janitor) Run(ctx context.Context) error {
	if j.cfg.Ticks == 0 && j.cfg.Raw == 0 && j.cfg.MaxBytes == 0 {
		j.log.Warn("retention disabled: the archive will grow without limit")
		<-ctx.Done()
		return nil
	}

	t := time.NewTicker(j.cfg.Interval)
	defer t.Stop()

	// Run once at startup rather than waiting a full interval. A restart
	// after a disk-full incident should recover immediately, not in an hour.
	j.once(ctx)

	for {
		select {
		case <-ctx.Done():
			return nil
		case <-t.C:
			j.once(ctx)
		}
	}
}

func (j *Janitor) once(ctx context.Context) {
	now := time.Now().UTC()

	var ticksBefore, rawBefore time.Time
	if j.cfg.Ticks > 0 {
		ticksBefore = now.Add(-j.cfg.Ticks)
	}
	if j.cfg.Raw > 0 {
		rawBefore = now.Add(-j.cfg.Raw)
	}

	if !ticksBefore.IsZero() || !rawBefore.IsZero() {
		ticks, raw, err := j.store.Prune(ctx, ticksBefore, rawBefore)
		if err != nil {
			j.log.Error("retention pass failed", "err", err)
			return
		}
		if ticks > 0 || raw > 0 {
			j.log.Info("pruned by age", "ticks", ticks, "raw_frames", raw)
		}
	}

	if j.cfg.MaxBytes > 0 {
		ticks, err := j.store.PruneToSize(ctx, j.cfg.MaxBytes)
		if err != nil {
			j.log.Error("size-based prune failed", "err", err)
			return
		}
		if ticks > 0 {
			// Hitting this means the time-based retention is too generous
			// for the volume — worth saying loudly rather than silently
			// discarding history.
			j.log.Warn("database over its size cap; dropped oldest days",
				"ticks_deleted", ticks, "max_bytes", j.cfg.MaxBytes)
		}
	}

	if size, err := j.store.SizeBytes(ctx); err == nil {
		j.log.Info("archive size", "bytes", size, "mb", size/(1<<20))
	}
}

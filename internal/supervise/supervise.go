// Package supervise restarts a failed component with backoff.
//
// This is what makes the pipeline survive an exchange disconnect. Reconnect
// is not "call connect again in a loop": without backoff you hammer a
// recovering exchange and get rate-limited, and without jitter every client
// in a datacenter retries in lockstep and creates a thundering herd out of
// what was a brief blip.
package supervise

import (
	"context"
	"errors"
	"log/slog"
	"math"
	"math/rand"
	"time"
)

// Policy tunes the retry loop.
type Policy struct {
	// InitialDelay before the first retry.
	InitialDelay time.Duration
	// MaxDelay caps exponential growth.
	MaxDelay time.Duration
	// Jitter is the fraction of the delay randomized, 0..1.
	Jitter float64
	// ResetAfter is how long a run must last to count as healthy and reset
	// the backoff. Without this, a component that connects and immediately
	// fails would retry at the initial delay forever.
	ResetAfter time.Duration
	// MaxAttempts stops after this many consecutive failures. Zero means
	// never give up.
	MaxAttempts int
}

func DefaultPolicy() Policy {
	return Policy{
		InitialDelay: time.Second,
		MaxDelay:     2 * time.Minute,
		Jitter:       0.3,
		ResetAfter:   60 * time.Second,
	}
}

// Stats reports supervision history, exposed via /metrics so a flapping
// connection is visible rather than merely noisy in the logs.
type Stats struct {
	Restarts    int       `json:"restarts"`
	Failures    int       `json:"failures"`
	LastError   string    `json:"last_error,omitempty"`
	LastStarted time.Time `json:"last_started"`
}

// Run calls fn repeatedly until ctx is cancelled or fn returns nil.
//
// fn is expected to block for as long as the component is healthy. It should
// hold no state across calls — each invocation must be able to rebuild
// whatever it needs, because that is exactly what a reconnect is.
func Run(ctx context.Context, name string, p Policy, log *slog.Logger, fn func(context.Context) error) error {
	if p.InitialDelay <= 0 {
		p = DefaultPolicy()
	}

	var attempt int
	for {
		started := time.Now()
		err := fn(ctx)

		switch {
		case ctx.Err() != nil:
			return ctx.Err()
		case err == nil:
			return nil
		case errors.Is(err, context.Canceled):
			return nil
		}

		// A run that lasted a while was a real connection, not a failing
		// retry — start the backoff over.
		if time.Since(started) >= p.ResetAfter {
			attempt = 0
		}
		attempt++

		if p.MaxAttempts > 0 && attempt >= p.MaxAttempts {
			log.Error("supervisor giving up", "component", name, "attempts", attempt, "err", err)
			return err
		}

		delay := backoff(p, attempt)
		log.Warn("component failed, restarting",
			"component", name, "attempt", attempt, "retry_in", delay.Round(time.Millisecond), "err", err)

		select {
		case <-time.After(delay):
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

// backoff is exponential with full-width jitter around the computed delay.
func backoff(p Policy, attempt int) time.Duration {
	d := float64(p.InitialDelay) * math.Pow(2, float64(attempt-1))
	if d > float64(p.MaxDelay) {
		d = float64(p.MaxDelay)
	}
	if p.Jitter > 0 {
		spread := d * p.Jitter
		d += (rand.Float64()*2 - 1) * spread
	}
	if d < float64(p.InitialDelay) {
		d = float64(p.InitialDelay)
	}
	return time.Duration(d)
}

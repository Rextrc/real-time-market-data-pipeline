// Package bus fans one stream of ticks out to many consumers running at
// different speeds.
//
// The central rule: one queue per subscriber, never a shared one. A shared
// queue couples unrelated consumers — the slowest sets the pace for all of
// them — which is head-of-line blocking and is almost always a bug.
package bus

import (
	"context"
	"fmt"

	"github.com/Rextrc/real-time-market-data-pipeline/internal/model"
)

// Policy decides what happens when a subscriber's queue is full.
//
// There is no globally correct answer, which is why this is declared per
// subscriber rather than configured once. The right policy follows from what
// the consumer is for.
type Policy string

const (
	// PolicyBlock makes the publisher wait for room.
	//
	// Only safe when the producer is one you control — a replay, a backfill.
	// Against a live exchange feed it is a trap: blocking the publisher
	// blocks the socket read, the kernel receive buffer fills, the TCP
	// window closes, the heartbeat is missed, and the exchange disconnects
	// you. Backpressure applied to a producer that cannot slow down becomes
	// a data gap instead.
	PolicyBlock Policy = "block"

	// PolicyDropNewest discards arriving ticks while the queue is full,
	// preserving the oldest. For sampling and non-critical telemetry.
	PolicyDropNewest Policy = "drop_newest"

	// PolicyDropOldest evicts the head to make room, preserving the newest.
	// For anything where staleness is worse than a gap.
	PolicyDropOldest Policy = "drop_oldest"

	// PolicyCoalesce keeps only the latest tick per instrument. Queue depth
	// is then bounded by the number of instruments rather than by the tick
	// rate, which makes it the only policy that is safe for an arbitrarily
	// slow consumer. The natural fit for a live UI feed.
	PolicyCoalesce Policy = "coalesce"
)

// Valid reports whether p is a known policy.
func (p Policy) Valid() bool {
	switch p {
	case PolicyBlock, PolicyDropNewest, PolicyDropOldest, PolicyCoalesce:
		return true
	}
	return false
}

// SubscriberSpec describes one consumer's appetite.
type SubscriberSpec struct {
	// Name identifies the subscriber in metrics and logs. Must be unique.
	Name string
	// Capacity is the queue depth. Zero uses DefaultCapacity.
	Capacity int
	// Policy decides behavior when the queue is full.
	Policy Policy
	// Instruments filters the subscription. Empty means everything.
	Instruments []model.InstrumentID
}

// DefaultCapacity is used when a spec asks for none.
const DefaultCapacity = 1024

// Validate checks the spec. Implementations call this so the rules live in
// one place.
func (s SubscriberSpec) Validate() error {
	if s.Name == "" {
		return fmt.Errorf("bus: subscriber needs a name")
	}
	if !s.Policy.Valid() {
		return fmt.Errorf("bus: subscriber %q has unknown policy %q", s.Name, s.Policy)
	}
	if s.Capacity < 0 {
		return fmt.Errorf("bus: subscriber %q has negative capacity", s.Name)
	}
	return nil
}

// Stats is a point-in-time view of one subscriber's queue.
type Stats struct {
	Name      string `json:"name"`
	Policy    Policy `json:"policy"`
	Capacity  int    `json:"capacity"`
	Depth     int    `json:"depth"`
	HighWater int    `json:"high_water"`
	Delivered uint64 `json:"delivered"`
	Dropped   uint64 `json:"dropped"`
	// Coalesced counts ticks superseded by a newer tick for the same
	// instrument before the consumer read them. Not data loss in the same
	// sense as Dropped: the latest value always survives.
	Coalesced uint64 `json:"coalesced"`
	// BlockedNanos is how long publishers have waited on this subscriber.
	// Non-zero here on a live feed is the warning sign that PolicyBlock is
	// pushing backpressure toward a producer that cannot absorb it.
	BlockedNanos uint64 `json:"blocked_nanos"`
}

// Subscription is a consumer's handle on the stream.
//
// Ticks() is deliberately never closed. Closing it would race with
// publishers still delivering — a send on a closed channel panics, and no
// amount of checking a flag first removes the window. Shutdown is signalled
// out of band on Done() instead, so consumers loop as:
//
//	for {
//		select {
//		case t := <-sub.Ticks():
//			...
//		case <-sub.Done():
//			return
//		}
//	}
type Subscription interface {
	Ticks() <-chan model.Tick
	// Done is closed when the subscription is shut down.
	Done() <-chan struct{}
	// Stats reports this subscriber's queue state.
	Stats() Stats
	// Close unsubscribes and releases the queue.
	Close()
}

// Bus is the fan-out point.
//
// Publish and Subscribe are deliberately coarse. This interface has to hold
// for an in-process implementation and for a broker-backed one, and those
// differ in delivery semantics — at-most-once here, at-least-once there. The
// interface cannot paper over that, so consumers are written idempotent and
// the difference stays a property of the implementation rather than a
// pretended equivalence.
type Bus interface {
	Publish(ctx context.Context, ticks []model.Tick) error
	Subscribe(spec SubscriberSpec) (Subscription, error)
	Stats() []Stats
	Close()
}

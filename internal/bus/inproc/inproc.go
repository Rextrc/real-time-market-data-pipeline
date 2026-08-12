// Package inproc implements bus.Bus with in-process queues.
//
// Fan-out costs a channel send per subscriber and no serialization, so it is
// measured in microseconds. The tradeoff is that every consumer shares this
// process's fate and nothing can redeliver: delivery is at-most-once.
package inproc

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Rextrc/real-time-market-data-pipeline/internal/bus"
	"github.com/Rextrc/real-time-market-data-pipeline/internal/model"
)

// Bus fans ticks out to independently-paced subscribers.
type Bus struct {
	mu     sync.RWMutex
	subs   map[string]*subscription
	closed bool
}

func New() *Bus {
	return &Bus{subs: make(map[string]*subscription)}
}

// Subscribe registers a consumer. Names must be unique.
func (b *Bus) Subscribe(spec bus.SubscriberSpec) (bus.Subscription, error) {
	if err := spec.Validate(); err != nil {
		return nil, err
	}

	b.mu.Lock()
	defer b.mu.Unlock()

	if b.closed {
		return nil, fmt.Errorf("bus: closed")
	}
	if _, exists := b.subs[spec.Name]; exists {
		return nil, fmt.Errorf("bus: subscriber %q already registered", spec.Name)
	}

	capacity := spec.Capacity
	if capacity == 0 {
		capacity = bus.DefaultCapacity
	}

	s := &subscription{
		bus:      b,
		spec:     spec,
		capacity: capacity,
		out:      make(chan model.Tick, capacity),
		done:     make(chan struct{}),
	}
	if len(spec.Instruments) > 0 {
		s.filter = make(map[model.InstrumentID]struct{}, len(spec.Instruments))
		for _, id := range spec.Instruments {
			s.filter[id] = struct{}{}
		}
	}
	if spec.Policy == bus.PolicyCoalesce {
		s.pending = make(map[model.InstrumentID]model.Tick)
		s.wake = make(chan struct{}, 1)
		go s.coalesceLoop()
	}

	b.subs[spec.Name] = s
	return s, nil
}

// Publish delivers to every subscriber according to its own policy.
//
// A slow subscriber cannot stall a fast one, because each has its own queue.
// The one exception is PolicyBlock, which is exactly the coupling that
// policy asks for.
func (b *Bus) Publish(ctx context.Context, ticks []model.Tick) error {
	if len(ticks) == 0 {
		return nil
	}

	b.mu.RLock()
	targets := make([]*subscription, 0, len(b.subs))
	for _, s := range b.subs {
		targets = append(targets, s)
	}
	b.mu.RUnlock()

	for _, s := range targets {
		for _, t := range ticks {
			if err := s.deliver(ctx, t); err != nil {
				return err
			}
		}
	}
	return nil
}

// Stats reports every subscriber's queue state.
func (b *Bus) Stats() []bus.Stats {
	b.mu.RLock()
	defer b.mu.RUnlock()

	out := make([]bus.Stats, 0, len(b.subs))
	for _, s := range b.subs {
		out = append(out, s.Stats())
	}
	return out
}

// Close shuts down every subscription.
func (b *Bus) Close() {
	b.mu.Lock()
	subs := make([]*subscription, 0, len(b.subs))
	for _, s := range b.subs {
		subs = append(subs, s)
	}
	b.subs = make(map[string]*subscription)
	b.closed = true
	b.mu.Unlock()

	for _, s := range subs {
		s.shutdown()
	}
}

func (b *Bus) remove(name string) {
	b.mu.Lock()
	delete(b.subs, name)
	b.mu.Unlock()
}

type subscription struct {
	bus      *Bus
	spec     bus.SubscriberSpec
	capacity int
	filter   map[model.InstrumentID]struct{}

	out  chan model.Tick
	done chan struct{}
	once sync.Once

	// Coalescing state: the latest tick per instrument, plus a wake signal.
	mu      sync.Mutex
	pending map[model.InstrumentID]model.Tick
	wake    chan struct{}

	delivered atomic.Uint64
	dropped   atomic.Uint64
	coalesced atomic.Uint64
	blockedNs atomic.Uint64
	highWater atomic.Int64
}

func (s *subscription) Ticks() <-chan model.Tick { return s.out }

func (s *subscription) Done() <-chan struct{} { return s.done }

func (s *subscription) Stats() bus.Stats {
	depth := len(s.out)
	if s.spec.Policy == bus.PolicyCoalesce {
		s.mu.Lock()
		depth += len(s.pending)
		s.mu.Unlock()
	}
	s.recordDepth(depth)

	return bus.Stats{
		Name:         s.spec.Name,
		Policy:       s.spec.Policy,
		Capacity:     s.capacity,
		Depth:        depth,
		HighWater:    int(s.highWater.Load()),
		Delivered:    s.delivered.Load(),
		Dropped:      s.dropped.Load(),
		Coalesced:    s.coalesced.Load(),
		BlockedNanos: s.blockedNs.Load(),
	}
}

func (s *subscription) recordDepth(d int) {
	for {
		hw := s.highWater.Load()
		if int64(d) <= hw || s.highWater.CompareAndSwap(hw, int64(d)) {
			return
		}
	}
}

func (s *subscription) Close() {
	s.bus.remove(s.spec.Name)
	s.shutdown()
}

func (s *subscription) shutdown() {
	// s.out is deliberately not closed. A publisher may be mid-deliver on
	// another goroutine, and a send on a closed channel panics — checking a
	// flag first only narrows the window, it does not remove it. Consumers
	// observe shutdown on Done() instead.
	s.once.Do(func() { close(s.done) })
}

func (s *subscription) wants(t model.Tick) bool {
	if s.filter == nil {
		return true
	}
	_, ok := s.filter[t.Instrument]
	return ok
}

func (s *subscription) deliver(ctx context.Context, t model.Tick) error {
	if !s.wants(t) {
		return nil
	}

	select {
	case <-s.done:
		return nil
	default:
	}

	switch s.spec.Policy {
	case bus.PolicyBlock:
		return s.deliverBlocking(ctx, t)
	case bus.PolicyDropNewest:
		s.deliverDropNewest(t)
	case bus.PolicyDropOldest:
		s.deliverDropOldest(t)
	case bus.PolicyCoalesce:
		s.deliverCoalesce(t)
	}
	return nil
}

func (s *subscription) deliverBlocking(ctx context.Context, t model.Tick) error {
	// Fast path: room available, no clock read.
	select {
	case s.out <- t:
		s.delivered.Add(1)
		s.recordDepth(len(s.out))
		return nil
	default:
	}

	// Slow path: the publisher is now paying for this consumer's slowness,
	// and that wait is measured so it shows up in /metrics rather than as a
	// mysterious ingest stall.
	start := time.Now()
	defer func() { s.blockedNs.Add(uint64(time.Since(start))) }()

	select {
	case s.out <- t:
		s.delivered.Add(1)
		s.recordDepth(len(s.out))
		return nil
	case <-s.done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (s *subscription) deliverDropNewest(t model.Tick) {
	select {
	case s.out <- t:
		s.delivered.Add(1)
		s.recordDepth(len(s.out))
	default:
		s.dropped.Add(1)
	}
}

func (s *subscription) deliverDropOldest(t model.Tick) {
	for {
		select {
		case s.out <- t:
			s.delivered.Add(1)
			s.recordDepth(len(s.out))
			return
		default:
		}

		// Evict one from the head and retry. The receive may lose the race
		// to the real consumer, in which case the retry simply succeeds.
		select {
		case <-s.out:
			s.dropped.Add(1)
		default:
		}

		select {
		case <-s.done:
			return
		default:
		}
	}
}

// deliverCoalesce keeps only the newest tick per instrument. Memory is
// bounded by the instrument count, so this is the one policy that survives
// an arbitrarily slow consumer without either dropping to zero or growing
// without limit.
func (s *subscription) deliverCoalesce(t model.Tick) {
	s.mu.Lock()
	if _, superseded := s.pending[t.Instrument]; superseded {
		s.coalesced.Add(1)
	}
	s.pending[t.Instrument] = t
	s.mu.Unlock()

	select {
	case s.wake <- struct{}{}:
	default: // a wake is already queued
	}
}

func (s *subscription) coalesceLoop() {
	for {
		select {
		case <-s.done:
			return
		case <-s.wake:
		}

		for {
			s.mu.Lock()
			var (
				next  model.Tick
				found bool
			)
			for id, t := range s.pending {
				next, found = t, true
				delete(s.pending, id)
				break
			}
			s.mu.Unlock()

			if !found {
				break
			}

			select {
			case s.out <- next:
				s.delivered.Add(1)
				s.recordDepth(len(s.out))
			case <-s.done:
				return
			}
		}
	}
}

var _ bus.Bus = (*Bus)(nil)

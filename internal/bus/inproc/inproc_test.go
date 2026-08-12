package inproc

import (
	"context"
	"testing"
	"time"

	"github.com/Rextrc/real-time-market-data-pipeline/internal/bus"
	"github.com/Rextrc/real-time-market-data-pipeline/internal/model"
)

func tick(id model.InstrumentID, seq int, price int64) model.Tick {
	return model.Tick{
		Venue:      model.VenueBinance,
		Instrument: id,
		Seq:        model.Seq(seq),
		Price:      model.Decimal{Unscaled: price, Scale: 2},
		Quantity:   model.Decimal{Unscaled: 1, Scale: 0},
		EventTime:  time.Unix(int64(seq), 0).UTC(),
	}
}

func TestFanOutDeliversToEverySubscriber(t *testing.T) {
	b := New()
	defer b.Close()

	names := []string{"a", "b", "c"}
	subs := make([]bus.Subscription, 0, len(names))
	for _, n := range names {
		s, err := b.Subscribe(bus.SubscriberSpec{Name: n, Capacity: 8, Policy: bus.PolicyDropNewest})
		if err != nil {
			t.Fatalf("subscribe %s: %v", n, err)
		}
		subs = append(subs, s)
	}

	if err := b.Publish(context.Background(), []model.Tick{tick("BTC-USDT", 1, 100)}); err != nil {
		t.Fatalf("publish: %v", err)
	}

	for i, s := range subs {
		select {
		case got := <-s.Ticks():
			if got.Seq != 1 {
				t.Errorf("subscriber %d got seq %d, want 1", i, got.Seq)
			}
		case <-time.After(time.Second):
			t.Errorf("subscriber %d received nothing", i)
		}
	}
}

// TestSlowSubscriberDoesNotStallFastOne is the M4 lesson as a test. With one
// queue per subscriber, a consumer that never reads costs only its own
// throughput; with a shared queue it would set the pace for everyone.
func TestSlowSubscriberDoesNotStallFastOne(t *testing.T) {
	b := New()
	defer b.Close()

	slow, err := b.Subscribe(bus.SubscriberSpec{Name: "slow", Capacity: 1, Policy: bus.PolicyDropOldest})
	if err != nil {
		t.Fatal(err)
	}
	fast, err := b.Subscribe(bus.SubscriberSpec{Name: "fast", Capacity: 1024, Policy: bus.PolicyDropNewest})
	if err != nil {
		t.Fatal(err)
	}
	_ = slow // deliberately never drained

	const n = 500
	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := 0; i < n; i++ {
			_ = b.Publish(context.Background(), []model.Tick{tick("BTC-USDT", i, int64(i))})
		}
	}()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("publishing stalled behind the un-drained subscriber")
	}

	var received int
	for {
		select {
		case <-fast.Ticks():
			received++
			continue
		default:
		}
		break
	}
	if received != n {
		t.Errorf("fast subscriber got %d of %d ticks", received, n)
	}
}

func TestDropNewestKeepsOldest(t *testing.T) {
	b := New()
	defer b.Close()

	sub, err := b.Subscribe(bus.SubscriberSpec{Name: "s", Capacity: 2, Policy: bus.PolicyDropNewest})
	if err != nil {
		t.Fatal(err)
	}

	for i := 1; i <= 5; i++ {
		_ = b.Publish(context.Background(), []model.Tick{tick("BTC-USDT", i, int64(i))})
	}

	got := drain(sub)
	if len(got) != 2 || got[0].Seq != 1 || got[1].Seq != 2 {
		t.Errorf("got seqs %v, want [1 2] — drop_newest must preserve the oldest", seqs(got))
	}
	if d := sub.Stats().Dropped; d != 3 {
		t.Errorf("Dropped = %d, want 3", d)
	}
}

func TestDropOldestKeepsNewest(t *testing.T) {
	b := New()
	defer b.Close()

	sub, err := b.Subscribe(bus.SubscriberSpec{Name: "s", Capacity: 2, Policy: bus.PolicyDropOldest})
	if err != nil {
		t.Fatal(err)
	}

	for i := 1; i <= 5; i++ {
		_ = b.Publish(context.Background(), []model.Tick{tick("BTC-USDT", i, int64(i))})
	}

	got := drain(sub)
	if len(got) != 2 || got[0].Seq != 4 || got[1].Seq != 5 {
		t.Errorf("got seqs %v, want [4 5] — drop_oldest must preserve the newest", seqs(got))
	}
}

// TestCoalesceBoundsMemoryByInstrumentCount is why this policy exists: with
// conflation the queue is bounded by how many instruments there are, not by
// how fast the market is moving, so an arbitrarily slow consumer is safe.
func TestCoalesceBoundsMemoryByInstrumentCount(t *testing.T) {
	b := New()
	defer b.Close()

	sub, err := b.Subscribe(bus.SubscriberSpec{Name: "ui", Capacity: 4, Policy: bus.PolicyCoalesce})
	if err != nil {
		t.Fatal(err)
	}

	// Flood two instruments with far more ticks than the queue could hold.
	for i := 1; i <= 2000; i++ {
		_ = b.Publish(context.Background(), []model.Tick{
			tick("BTC-USDT", i, int64(i)),
			tick("ETH-USDT", i, int64(i)),
		})
	}

	// The newest price for each instrument must survive.
	deadline := time.After(3 * time.Second)
	latest := map[model.InstrumentID]model.Seq{}
	for len(latest) < 2 {
		select {
		case tk := <-sub.Ticks():
			if tk.Seq > latest[tk.Instrument] {
				latest[tk.Instrument] = tk.Seq
			}
		case <-deadline:
			t.Fatalf("only saw %d instruments", len(latest))
		}
	}

	stats := sub.Stats()
	if stats.Dropped != 0 {
		t.Errorf("Dropped = %d, want 0 — coalescing supersedes, it does not drop", stats.Dropped)
	}
	if stats.Coalesced == 0 {
		t.Error("Coalesced = 0, want > 0")
	}
}

func TestBlockPolicyMakesPublisherWait(t *testing.T) {
	b := New()
	defer b.Close()

	sub, err := b.Subscribe(bus.SubscriberSpec{Name: "s", Capacity: 1, Policy: bus.PolicyBlock})
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	// Capacity 1: the first send fits, the second blocks until the context
	// expires. That stall is the whole point of the policy — and the reason
	// it must never be pointed at an exchange feed.
	err = b.Publish(ctx, []model.Tick{tick("BTC-USDT", 1, 1), tick("BTC-USDT", 2, 2)})
	if err == nil {
		t.Fatal("expected Publish to block until the context expired")
	}
	if sub.Stats().BlockedNanos == 0 {
		t.Error("BlockedNanos = 0; blocked time must be measured so it shows up in metrics")
	}
}

func TestInstrumentFilter(t *testing.T) {
	b := New()
	defer b.Close()

	sub, err := b.Subscribe(bus.SubscriberSpec{
		Name: "btc-only", Capacity: 16, Policy: bus.PolicyDropNewest,
		Instruments: []model.InstrumentID{"BTC-USDT"},
	})
	if err != nil {
		t.Fatal(err)
	}

	_ = b.Publish(context.Background(), []model.Tick{
		tick("ETH-USDT", 1, 1), tick("BTC-USDT", 2, 2), tick("SOL-USDT", 3, 3),
	})

	got := drain(sub)
	if len(got) != 1 || got[0].Instrument != "BTC-USDT" {
		t.Errorf("got %v, want only BTC-USDT", got)
	}
}

func TestDuplicateSubscriberRejected(t *testing.T) {
	b := New()
	defer b.Close()

	spec := bus.SubscriberSpec{Name: "dup", Policy: bus.PolicyDropNewest}
	if _, err := b.Subscribe(spec); err != nil {
		t.Fatal(err)
	}
	if _, err := b.Subscribe(spec); err == nil {
		t.Error("expected an error registering a duplicate subscriber name")
	}
}

func TestInvalidPolicyRejected(t *testing.T) {
	b := New()
	defer b.Close()

	if _, err := b.Subscribe(bus.SubscriberSpec{Name: "x", Policy: "nonsense"}); err == nil {
		t.Error("expected an error for an unknown policy")
	}
}

// TestPublishAfterCloseDoesNotPanic guards the send-on-closed-channel bug
// that closing the output channel on shutdown would introduce.
func TestPublishAfterCloseDoesNotPanic(t *testing.T) {
	b := New()
	sub, err := b.Subscribe(bus.SubscriberSpec{Name: "s", Capacity: 4, Policy: bus.PolicyDropOldest})
	if err != nil {
		t.Fatal(err)
	}

	b.Close()

	select {
	case <-sub.Done():
	default:
		t.Error("Done() should be closed after the bus shuts down")
	}

	// A publisher mid-flight when Close lands must not panic.
	for i := 0; i < 100; i++ {
		_ = b.Publish(context.Background(), []model.Tick{tick("BTC-USDT", i, int64(i))})
	}
}

func drain(s bus.Subscription) []model.Tick {
	var out []model.Tick
	for {
		select {
		case t := <-s.Ticks():
			out = append(out, t)
		default:
			return out
		}
	}
}

func seqs(ts []model.Tick) []model.Seq {
	out := make([]model.Seq, len(ts))
	for i, t := range ts {
		out[i] = t.Seq
	}
	return out
}

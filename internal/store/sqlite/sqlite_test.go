package sqlite

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"github.com/Rextrc/real-time-market-data-pipeline/internal/model"
	"github.com/Rextrc/real-time-market-data-pipeline/internal/store"
)

func open(t *testing.T) *DB {
	t.Helper()
	db, err := Open(context.Background(), filepath.Join(t.TempDir(), "test.db"), DurabilityOff)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { db.Close() })
	return db
}

func mkTick(id model.InstrumentID, tradeID string, tsMillis int64, price string) model.Tick {
	p, err := model.ParseDecimal(price)
	if err != nil {
		panic(err)
	}
	return model.Tick{
		Venue: model.VenueBinance, Instrument: id, VenueTradeID: tradeID, Seq: 1,
		EventTime: time.UnixMilli(tsMillis).UTC(),
		RecvTime:  time.UnixMilli(tsMillis + 50).UTC(),
		Price:     p,
		Quantity:  model.Decimal{Unscaled: 125, Scale: 3},
		Side:      model.SideBuy,
	}
}

func TestAppendAndReadRoundTrip(t *testing.T) {
	db := open(t)
	ctx := context.Background()

	in := mkTick("BTC-USDT", "1", 1710000000000, "68420.51000000")
	if n, err := db.Append(ctx, []model.Tick{in}); err != nil || n != 1 {
		t.Fatalf("Append = %d, %v", n, err)
	}

	got, err := db.Latest(ctx, model.VenueBinance, "BTC-USDT")
	if err != nil {
		t.Fatalf("Latest: %v", err)
	}

	// Exactness through the storage layer is the whole point: a price that
	// round-trips as a float would be silently wrong here.
	if got.Price.String() != in.Price.String() {
		t.Errorf("Price = %q, want %q", got.Price, in.Price)
	}
	if got.Quantity.String() != in.Quantity.String() {
		t.Errorf("Quantity = %q, want %q", got.Quantity, in.Quantity)
	}
	if !got.EventTime.Equal(in.EventTime) {
		t.Errorf("EventTime = %v, want %v", got.EventTime, in.EventTime)
	}
	if got.Side != in.Side || got.VenueTradeID != in.VenueTradeID {
		t.Errorf("got %+v, want side=%v id=%s", got, in.Side, in.VenueTradeID)
	}
}

// TestAppendIsIdempotent is the property every consumer depends on once
// delivery becomes at-least-once. A reconnect that replays overlapping trades
// must not double-count them.
func TestAppendIsIdempotent(t *testing.T) {
	db := open(t)
	ctx := context.Background()

	batch := []model.Tick{
		mkTick("BTC-USDT", "1", 1710000000000, "100.00"),
		mkTick("BTC-USDT", "2", 1710000001000, "101.00"),
	}

	if n, _ := db.Append(ctx, batch); n != 2 {
		t.Fatalf("first append stored %d, want 2", n)
	}
	n, err := db.Append(ctx, batch)
	if err != nil {
		t.Fatalf("re-append: %v", err)
	}
	if n != 0 {
		t.Errorf("re-append stored %d rows, want 0 — replay must be a no-op", n)
	}

	page, err := db.Ticks(ctx, store.Query{Instrument: "BTC-USDT"})
	if err != nil {
		t.Fatal(err)
	}
	if len(page.Ticks) != 2 {
		t.Errorf("archive holds %d ticks after replay, want 2", len(page.Ticks))
	}
}

func TestTicksTimeRangeAndOrder(t *testing.T) {
	db := open(t)
	ctx := context.Background()

	var batch []model.Tick
	for i := 0; i < 10; i++ {
		batch = append(batch, mkTick("BTC-USDT", string(rune('a'+i)), 1710000000000+int64(i)*1000, "100.00"))
	}
	if _, err := db.Append(ctx, batch); err != nil {
		t.Fatal(err)
	}

	page, err := db.Ticks(ctx, store.Query{
		Instrument: "BTC-USDT",
		Start:      time.UnixMilli(1710000003000).UTC(),
		End:        time.UnixMilli(1710000007000).UTC(),
	})
	if err != nil {
		t.Fatal(err)
	}
	// Start inclusive, End exclusive → indices 3,4,5,6.
	if len(page.Ticks) != 4 {
		t.Fatalf("got %d ticks, want 4", len(page.Ticks))
	}
	for i := 1; i < len(page.Ticks); i++ {
		if page.Ticks[i].EventTime.Before(page.Ticks[i-1].EventTime) {
			t.Error("results are not in ascending event-time order")
		}
	}
}

// TestKeysetPagingCoversEveryRowExactlyOnce is why paging is cursor-based
// rather than OFFSET-based: on a live feed, rows arriving mid-scan would make
// an offset skip or repeat records.
func TestKeysetPagingCoversEveryRowExactlyOnce(t *testing.T) {
	db := open(t)
	ctx := context.Background()

	const total = 25
	var batch []model.Tick
	for i := 0; i < total; i++ {
		batch = append(batch, mkTick("BTC-USDT", string(rune('A'+i)), 1710000000000+int64(i)*1000, "100.00"))
	}
	if _, err := db.Append(ctx, batch); err != nil {
		t.Fatal(err)
	}

	seen := map[string]int{}
	q := store.Query{Instrument: "BTC-USDT", Limit: 7}
	for pages := 0; ; pages++ {
		if pages > 10 {
			t.Fatal("paging did not terminate")
		}
		page, err := db.Ticks(ctx, q)
		if err != nil {
			t.Fatal(err)
		}
		for _, tk := range page.Ticks {
			seen[tk.VenueTradeID]++
		}
		if !page.HasMore {
			break
		}
		q.Cursor = page.NextCursor
	}

	if len(seen) != total {
		t.Errorf("saw %d distinct rows, want %d", len(seen), total)
	}
	for id, n := range seen {
		if n != 1 {
			t.Errorf("row %s returned %d times, want once", id, n)
		}
	}
}

func TestLatestNotFound(t *testing.T) {
	db := open(t)
	if _, err := db.Latest(context.Background(), model.VenueBinance, "NOPE-USDT"); !errors.Is(err, store.ErrNotFound) {
		t.Errorf("err = %v, want store.ErrNotFound", err)
	}
}

func TestInstrumentsSummary(t *testing.T) {
	db := open(t)
	ctx := context.Background()

	_, err := db.Append(ctx, []model.Tick{
		mkTick("BTC-USDT", "1", 1710000000000, "100.00"),
		mkTick("BTC-USDT", "2", 1710000005000, "101.00"),
		mkTick("ETH-USDT", "3", 1710000002000, "50.00"),
	})
	if err != nil {
		t.Fatal(err)
	}

	stats, err := db.Instruments(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(stats) != 2 {
		t.Fatalf("got %d instruments, want 2", len(stats))
	}
	for _, s := range stats {
		if s.Instrument == "BTC-USDT" {
			if s.Ticks != 2 {
				t.Errorf("BTC ticks = %d, want 2", s.Ticks)
			}
			if !s.First.Equal(time.UnixMilli(1710000000000).UTC()) {
				t.Errorf("BTC first = %v", s.First)
			}
			if !s.Last.Equal(time.UnixMilli(1710000005000).UTC()) {
				t.Errorf("BTC last = %v", s.Last)
			}
		}
	}
}

func TestRawFramesPersisted(t *testing.T) {
	db := open(t)
	ctx := context.Background()

	err := db.AppendRawBatch(ctx, []RawFrame{
		{Venue: model.VenueBinance, RecvTime: time.Now().UTC(), Payload: []byte(`{"a":1}`)},
		{Venue: model.VenueBinance, RecvTime: time.Now().UTC(), Payload: []byte(`{"b":2}`)},
	})
	if err != nil {
		t.Fatalf("AppendRawBatch: %v", err)
	}

	var n int
	if err := db.db.QueryRow(`SELECT COUNT(*) FROM raw_frames`).Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != 2 {
		t.Errorf("raw_frames holds %d rows, want 2", n)
	}
}

func TestUnknownDurabilityRejected(t *testing.T) {
	_, err := Open(context.Background(), filepath.Join(t.TempDir(), "x.db"), Durability("sometimes"))
	if err == nil {
		t.Error("expected an error for an unrecognized durability setting")
	}
}

func TestPruneByAge(t *testing.T) {
	db := open(t)
	ctx := context.Background()

	old := time.Now().UTC().Add(-48 * time.Hour)
	recent := time.Now().UTC().Add(-1 * time.Hour)

	var batch []model.Tick
	for i := 0; i < 5; i++ {
		batch = append(batch, mkTick("BTC-USDT", "old"+string(rune('a'+i)), old.UnixMilli()+int64(i), "100.00"))
	}
	for i := 0; i < 5; i++ {
		batch = append(batch, mkTick("BTC-USDT", "new"+string(rune('a'+i)), recent.UnixMilli()+int64(i), "100.00"))
	}
	if _, err := db.Append(ctx, batch); err != nil {
		t.Fatal(err)
	}
	if err := db.AppendRawBatch(ctx, []RawFrame{
		{Venue: model.VenueBinance, RecvTime: old, Payload: []byte("{}")},
		{Venue: model.VenueBinance, RecvTime: recent, Payload: []byte("{}")},
	}); err != nil {
		t.Fatal(err)
	}

	cutoff := time.Now().UTC().Add(-24 * time.Hour)
	ticks, raw, err := db.Prune(ctx, cutoff, cutoff)
	if err != nil {
		t.Fatalf("Prune: %v", err)
	}
	if ticks != 5 {
		t.Errorf("deleted %d ticks, want 5", ticks)
	}
	if raw != 1 {
		t.Errorf("deleted %d raw frames, want 1", raw)
	}

	page, err := db.Ticks(ctx, store.Query{Instrument: "BTC-USDT"})
	if err != nil {
		t.Fatal(err)
	}
	if len(page.Ticks) != 5 {
		t.Errorf("%d ticks remain, want 5", len(page.Ticks))
	}
	for _, tk := range page.Ticks {
		if tk.EventTime.Before(cutoff) {
			t.Errorf("tick %s survived the cutoff", tk.VenueTradeID)
		}
	}
}

func TestPruneZeroCutoffKeepsEverything(t *testing.T) {
	db := open(t)
	ctx := context.Background()

	if _, err := db.Append(ctx, []model.Tick{
		mkTick("BTC-USDT", "1", time.Now().Add(-999*time.Hour).UnixMilli(), "100.00"),
	}); err != nil {
		t.Fatal(err)
	}

	// A zero cutoff means "keep forever" — it must not be read as
	// "delete everything before the zero time", and certainly not as
	// "delete everything".
	ticks, raw, err := db.Prune(ctx, time.Time{}, time.Time{})
	if err != nil {
		t.Fatal(err)
	}
	if ticks != 0 || raw != 0 {
		t.Errorf("zero cutoff deleted %d ticks and %d raw frames, want 0 and 0", ticks, raw)
	}
}

// TestPruneToSizeReclaimsSpace is the last-resort guard for a fixed-size
// volume. It must actually shrink the file, not just the row count —
// deleting rows alone leaves the pages on SQLite's freelist.
func TestPruneToSizeReclaimsSpace(t *testing.T) {
	db := open(t)
	ctx := context.Background()

	// Several distinct days so there is something to drop partition-wise.
	base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	for day := 0; day < 6; day++ {
		var batch []model.Tick
		for i := 0; i < 2000; i++ {
			ts := base.AddDate(0, 0, day).Add(time.Duration(i) * time.Second)
			batch = append(batch, mkTick("BTC-USDT",
				fmt.Sprintf("d%d-%d", day, i), ts.UnixMilli(), "68420.51000000"))
		}
		if _, err := db.Append(ctx, batch); err != nil {
			t.Fatal(err)
		}
	}

	before, err := db.SizeBytes(ctx)
	if err != nil {
		t.Fatal(err)
	}
	target := before / 2

	deleted, err := db.PruneToSize(ctx, target)
	if err != nil {
		t.Fatalf("PruneToSize: %v", err)
	}
	if deleted == 0 {
		t.Fatal("PruneToSize deleted nothing")
	}

	after, err := db.SizeBytes(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if after >= before {
		t.Errorf("size did not shrink: %d -> %d (freed pages must be returned, not left on the freelist)",
			before, after)
	}

	// The oldest day must be the one that went.
	page, err := db.Ticks(ctx, store.Query{Instrument: "BTC-USDT", Limit: 1})
	if err != nil {
		t.Fatal(err)
	}
	if len(page.Ticks) > 0 && page.Ticks[0].EventTime.Before(base.AddDate(0, 0, 1)) {
		t.Error("oldest day survived; pruning must drop oldest-first")
	}
}

func TestPruneToSizeNoopWhenUnderCap(t *testing.T) {
	db := open(t)
	ctx := context.Background()

	if _, err := db.Append(ctx, []model.Tick{mkTick("BTC-USDT", "1", 1710000000000, "100.00")}); err != nil {
		t.Fatal(err)
	}
	deleted, err := db.PruneToSize(ctx, 1<<30)
	if err != nil {
		t.Fatal(err)
	}
	if deleted != 0 {
		t.Errorf("deleted %d rows while under the cap, want 0", deleted)
	}
}

// Package store defines durable persistence for ticks. The interfaces here
// are what consumers and the API depend on; concrete engines live in
// subpackages and are chosen in cmd/mdp.
package store

import (
	"context"
	"errors"
	"time"

	"github.com/Rextrc/real-time-market-data-pipeline/internal/model"
)

// ErrNotFound is returned when a lookup has no rows.
var ErrNotFound = errors.New("store: not found")

// Appender writes ticks durably.
//
// Append must be idempotent: re-appending a tick that is already stored is a
// no-op, not a duplicate and not an error. Every delivery path in this system
// is allowed to redeliver — a reconnect replays overlapping trades, and a
// broker with at-least-once semantics replays anything unacknowledged — so
// idempotency here is what keeps the archive correct rather than merely
// approximately correct.
type Appender interface {
	Append(ctx context.Context, ticks []model.Tick) (stored int, err error)
	Close() error
}

// RawAppender persists undecoded exchange frames alongside the normalized
// ticks. This is the cheapest insurance in the system: when a decoder bug is
// found, the whole history can be re-normalized instead of being lost.
type RawAppender interface {
	AppendRaw(ctx context.Context, venue model.VenueID, recv time.Time, payload []byte) error
}

// Query selects a window of ticks. Zero values mean "unbounded".
type Query struct {
	Venue      model.VenueID
	Instrument model.InstrumentID
	Start      time.Time // inclusive, by event time
	End        time.Time // exclusive, by event time
	Limit      int
	// Cursor pages forward. Pass the NextCursor from the previous page.
	Cursor Cursor
	// Descending returns newest first. Paging still moves away from the
	// starting edge in both directions.
	Descending bool
}

// Cursor is an opaque forward-paging position. It is keyed on event time
// plus trade id rather than an offset, so inserts arriving mid-page cannot
// cause a row to be skipped or repeated.
type Cursor struct {
	EventTimeNS int64
	TradeID     string
}

func (c Cursor) IsZero() bool { return c.EventTimeNS == 0 && c.TradeID == "" }

// Page is one result window.
type Page struct {
	Ticks      []model.Tick
	NextCursor Cursor
	HasMore    bool
}

// InstrumentStat summarizes what the archive holds for one instrument.
type InstrumentStat struct {
	Venue      model.VenueID      `json:"venue"`
	Instrument model.InstrumentID `json:"instrument"`
	Ticks      int64              `json:"ticks"`
	First      time.Time          `json:"first_event_time"`
	Last       time.Time          `json:"last_event_time"`
}

// Reader serves the archive.
type Reader interface {
	Ticks(ctx context.Context, q Query) (Page, error)
	Latest(ctx context.Context, venue model.VenueID, id model.InstrumentID) (model.Tick, error)
	Instruments(ctx context.Context) ([]InstrumentStat, error)
}

// Store is both halves.
type Store interface {
	Appender
	RawAppender
	Reader
}

// DefaultLimit caps an unbounded query so a missing limit cannot pull the
// entire archive into memory.
const DefaultLimit = 1000

// MaxLimit caps what a caller may ask for.
const MaxLimit = 50000

// NormalizeLimit applies the default and ceiling.
func NormalizeLimit(n int) int {
	switch {
	case n <= 0:
		return DefaultLimit
	case n > MaxLimit:
		return MaxLimit
	default:
		return n
	}
}

// Package model holds the internal vocabulary of the pipeline. It imports
// nothing outside the standard library, and nothing in it knows how ticks
// are transported, encoded, or stored.
package model

import (
	"fmt"
	"time"
)

// VenueID names an exchange.
type VenueID string

// Venue IDs live here rather than in each adapter package so that decoders,
// stores, and consumers can refer to a venue without importing its
// implementation — which would pull a websocket client into the import graph
// of packages that have no business having one. These strings are written
// into every stored row, so they are permanent.
const (
	VenueBinance  VenueID = "binance"
	VenueCoinbase VenueID = "coinbase"
)

// InstrumentID is the canonical, venue-independent name for a tradable pair,
// formatted BASE-QUOTE (e.g. "BTC-USDT"). Venue-specific spellings such as
// "btcusdt" or "XBT/USD" are translated at the venue boundary and never reach
// the bus or the store.
type InstrumentID string

// Side is the aggressor's direction: the side that crossed the spread.
type Side uint8

const (
	SideUnknown Side = iota
	SideBuy
	SideSell
)

func (s Side) String() string {
	switch s {
	case SideBuy:
		return "buy"
	case SideSell:
		return "sell"
	default:
		return "unknown"
	}
}

// Seq is a per-(venue, instrument) monotonic counter assigned at ingest. It
// establishes our own ordering independent of any exchange's numbering, and
// it is what downstream consumers deduplicate on when delivery becomes
// at-least-once.
type Seq uint64

// Tick is one trade, normalized.
//
// Three timestamps, deliberately:
//
//	EventTime - when the venue says it happened. Market semantics.
//	RecvTime  - when this process read it off the socket. Pipeline diagnostics.
//	Elapsed   - measured with the monotonic clock, never wall time.
//
// Wall-clock time is never a sort key: it can jump backwards under NTP.
// Ordering comes from Seq.
type Tick struct {
	Venue      VenueID
	Instrument InstrumentID
	Seq        Seq

	EventTime time.Time
	RecvTime  time.Time

	Price    Decimal
	Quantity Decimal
	Side     Side

	// VenueTradeID is the exchange's own identifier for this trade, kept
	// verbatim. It is the key for detecting gaps and duplicates across a
	// reconnect, and no two venues agree on its type, so it stays a string.
	VenueTradeID string
}

// Lag is how long the tick spent in flight between the venue's clock and ours.
// It compares two different clocks and so is only ever a diagnostic, not a
// measurement.
func (t Tick) Lag() time.Duration { return t.RecvTime.Sub(t.EventTime) }

func (t Tick) String() string {
	return fmt.Sprintf("%s %-10s seq=%-6d %s %s @ %s  lag=%s",
		t.Venue, t.Instrument, t.Seq, t.Side, t.Quantity, t.Price,
		t.Lag().Truncate(time.Millisecond))
}

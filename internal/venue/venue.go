// Package venue defines the ingest boundary: how bytes get from an exchange
// into the pipeline. Nothing here decodes those bytes — that is normalize's
// job — so a venue implementation is purely transport plus subscription
// management.
package venue

import (
	"context"
	"time"

	"github.com/Rextrc/real-time-market-data-pipeline/internal/model"
)

// RawMessage is one frame as it came off the wire, plus the local time it
// arrived. The payload is kept verbatim: it is persisted alongside the
// normalized tick (M3) so the whole history can be re-decoded when a decoder
// bug turns up.
type RawMessage struct {
	Venue    model.VenueID
	RecvTime time.Time
	Payload  []byte
}

// Venue is a source of raw exchange frames.
//
// Stream blocks. It returns nil only when ctx is cancelled, and an error for
// anything else — a dial failure, a dropped connection, a missed heartbeat.
// It does not reconnect; that is the supervisor's job in M6, and keeping the
// retry policy out of here is what lets M6 be additive.
//
// This differs from the sketch in DESIGN.md, which returned a channel. A
// blocking call that owns no goroutines composes directly with errgroup, has
// exactly one way to report failure, and cannot leak a reader on reconnect —
// which is the specific bug M6 goes looking for.
type Venue interface {
	ID() model.VenueID

	Stream(ctx context.Context, instruments []model.InstrumentID, out chan<- RawMessage) error
}

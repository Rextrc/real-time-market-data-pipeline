// Package normalize turns venue-specific frames into model.Ticks. It is the
// only place that knows any exchange's wire format, and it is where the
// pipeline's ordering is established.
package normalize

import (
	"errors"
	"sync"

	"github.com/Rextrc/real-time-market-data-pipeline/internal/model"
	"github.com/Rextrc/real-time-market-data-pipeline/internal/venue"
)

// ErrIgnored means the frame was well-formed but carried nothing we model —
// a subscription acknowledgement, a heartbeat, an event type we don't handle.
// It is not a failure and must not be counted as one.
var ErrIgnored = errors.New("normalize: frame carries no ticks")

// Decoder converts one raw frame into zero or more ticks, with Seq unset.
type Decoder interface {
	Venue() model.VenueID
	Decode(msg venue.RawMessage) ([]model.Tick, error)
}

// Sequencer assigns the per-(venue, instrument) monotonic Seq that gives the
// pipeline its own ordering, independent of any exchange's numbering.
//
// Seq is deliberately not derived from the venue's trade ID: venues restart
// numbering, reuse IDs across instruments, and disagree on type. Ours is the
// one counter every consumer can rely on.
type Sequencer struct {
	mu   sync.Mutex
	next map[seqKey]model.Seq
}

type seqKey struct {
	venue      model.VenueID
	instrument model.InstrumentID
}

func NewSequencer() *Sequencer {
	return &Sequencer{next: make(map[seqKey]model.Seq)}
}

// Assign stamps each tick in place.
func (s *Sequencer) Assign(ticks []model.Tick) {
	s.mu.Lock()
	defer s.mu.Unlock()

	for i := range ticks {
		k := seqKey{ticks[i].Venue, ticks[i].Instrument}
		s.next[k]++
		ticks[i].Seq = s.next[k]
	}
}

// Normalizer routes frames to the right decoder and sequences the result.
type Normalizer struct {
	decoders map[model.VenueID]Decoder
	seq      *Sequencer
}

func New(decoders ...Decoder) *Normalizer {
	m := make(map[model.VenueID]Decoder, len(decoders))
	for _, d := range decoders {
		m[d.Venue()] = d
	}
	return &Normalizer{decoders: m, seq: NewSequencer()}
}

// Normalize returns the ticks in a frame. It may return ErrIgnored, which
// callers should treat as "nothing to do" rather than as an error.
func (n *Normalizer) Normalize(msg venue.RawMessage) ([]model.Tick, error) {
	d, ok := n.decoders[msg.Venue]
	if !ok {
		return nil, errors.New("normalize: no decoder registered for venue " + string(msg.Venue))
	}

	ticks, err := d.Decode(msg)
	if err != nil {
		return nil, err
	}
	if len(ticks) == 0 {
		return nil, ErrIgnored
	}

	n.seq.Assign(ticks)
	return ticks, nil
}

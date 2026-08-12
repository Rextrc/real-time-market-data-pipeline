// Package instrument translates between canonical InstrumentIDs and the
// spellings individual venues use. It is deliberately a dumb bidirectional
// map: the venue-specific tables live in each venue package and are
// registered by cmd/mdp at startup, so no package here imports a venue.
package instrument

import (
	"fmt"
	"strings"
	"sync"

	"github.com/Rextrc/real-time-market-data-pipeline/internal/model"
)

// Mapping ties one canonical instrument to one venue's spelling of it.
type Mapping struct {
	Venue      model.VenueID
	Instrument model.InstrumentID
	// Symbol is the venue's wire spelling, exactly as it appears in its API
	// (e.g. "BTCUSDT" for Binance, "BTC-USD" for Coinbase).
	Symbol string
}

// Registry resolves canonical <-> venue symbols. Safe for concurrent use.
type Registry struct {
	mu       sync.RWMutex
	toVenue  map[model.VenueID]map[model.InstrumentID]string
	toCanoni map[model.VenueID]map[string]model.InstrumentID
}

func NewRegistry() *Registry {
	return &Registry{
		toVenue:  make(map[model.VenueID]map[model.InstrumentID]string),
		toCanoni: make(map[model.VenueID]map[string]model.InstrumentID),
	}
}

// Register adds mappings. Conflicting entries are an error rather than a
// silent overwrite: two canonical IDs pointing at one venue symbol would
// corrupt the archive in a way that is not recoverable after the fact.
func (r *Registry) Register(ms ...Mapping) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	for _, m := range ms {
		if m.Venue == "" || m.Instrument == "" || m.Symbol == "" {
			return fmt.Errorf("instrument: incomplete mapping %+v", m)
		}
		key := venueKey(m.Symbol)

		if r.toVenue[m.Venue] == nil {
			r.toVenue[m.Venue] = make(map[model.InstrumentID]string)
			r.toCanoni[m.Venue] = make(map[string]model.InstrumentID)
		}
		if existing, ok := r.toVenue[m.Venue][m.Instrument]; ok && existing != m.Symbol {
			return fmt.Errorf("instrument: %s/%s already maps to %q, refusing to remap to %q",
				m.Venue, m.Instrument, existing, m.Symbol)
		}
		if existing, ok := r.toCanoni[m.Venue][key]; ok && existing != m.Instrument {
			return fmt.Errorf("instrument: %s symbol %q already maps to %s, refusing to remap to %s",
				m.Venue, m.Symbol, existing, m.Instrument)
		}
		r.toVenue[m.Venue][m.Instrument] = m.Symbol
		r.toCanoni[m.Venue][key] = m.Instrument
	}
	return nil
}

// Symbol returns the venue's spelling of a canonical instrument.
func (r *Registry) Symbol(v model.VenueID, id model.InstrumentID) (string, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	s, ok := r.toVenue[v][id]
	return s, ok
}

// Canonical returns the canonical instrument for a venue's spelling. Lookup
// is case-insensitive because venues are not consistent about case even
// within a single API (Binance streams are lowercase, payloads uppercase).
func (r *Registry) Canonical(v model.VenueID, symbol string) (model.InstrumentID, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	id, ok := r.toCanoni[v][venueKey(symbol)]
	return id, ok
}

func venueKey(symbol string) string { return strings.ToUpper(symbol) }

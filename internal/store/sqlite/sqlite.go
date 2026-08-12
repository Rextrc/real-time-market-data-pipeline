// Package sqlite implements store.Store on SQLite.
//
// SQLite is the right hot tier for a single machine: one file, no server,
// and with WAL mode a writer does not block readers. The API can serve
// queries while ingest is writing at full rate.
package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"

	_ "modernc.org/sqlite" // pure-Go driver: no cgo, so cross-compilation stays trivial

	"github.com/Rextrc/real-time-market-data-pipeline/internal/model"
	"github.com/Rextrc/real-time-market-data-pipeline/internal/store"
)

const schema = `
PRAGMA journal_mode = WAL;
PRAGMA synchronous = NORMAL;
PRAGMA busy_timeout = 5000;
PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS ticks (
	venue          TEXT    NOT NULL,
	instrument     TEXT    NOT NULL,
	venue_trade_id TEXT    NOT NULL,
	seq            INTEGER NOT NULL,
	event_ts_ns    INTEGER NOT NULL,
	recv_ts_ns     INTEGER NOT NULL,
	price_unscaled INTEGER NOT NULL,
	price_scale    INTEGER NOT NULL,
	qty_unscaled   INTEGER NOT NULL,
	qty_scale      INTEGER NOT NULL,
	side           INTEGER NOT NULL,
	day            TEXT    NOT NULL,
	PRIMARY KEY (venue, instrument, venue_trade_id)
);

CREATE INDEX IF NOT EXISTS ticks_by_time
	ON ticks (venue, instrument, event_ts_ns, venue_trade_id);
CREATE INDEX IF NOT EXISTS ticks_by_day ON ticks (day);

CREATE TABLE IF NOT EXISTS raw_frames (
	venue      TEXT    NOT NULL,
	recv_ts_ns INTEGER NOT NULL,
	day        TEXT    NOT NULL,
	payload    BLOB    NOT NULL
);
CREATE INDEX IF NOT EXISTS raw_by_time ON raw_frames (venue, recv_ts_ns);
CREATE INDEX IF NOT EXISTS raw_by_day ON raw_frames (day);
`

// Durability selects the fsync policy. This is the dial from M3, named so
// that it is a deliberate choice rather than a default nobody looked at.
type Durability string

const (
	// DurabilityFull fsyncs on every commit. No data loss on power failure,
	// at a large throughput cost.
	DurabilityFull Durability = "full"
	// DurabilityNormal fsyncs the WAL at checkpoints. A power failure can
	// lose recent commits; a process crash cannot. This is the default and
	// the right trade for market data, where the cost of losing the last
	// few hundred milliseconds is low.
	DurabilityNormal Durability = "normal"
	// DurabilityOff never fsyncs. Fastest, and a power failure can corrupt
	// the database. Useful only for backfill and benchmarks.
	DurabilityOff Durability = "off"
)

// DB is a SQLite-backed store.
type DB struct {
	db *sql.DB
}

// Open creates or opens the database at path and applies the schema.
func Open(ctx context.Context, path string, d Durability) (*DB, error) {
	db, err := sql.Open("sqlite", path+"?_pragma=busy_timeout(5000)")
	if err != nil {
		return nil, fmt.Errorf("sqlite: open %s: %w", path, err)
	}

	// A single writer connection avoids SQLITE_BUSY under concurrent
	// batched writes. Reads still run concurrently thanks to WAL.
	db.SetMaxOpenConns(1)

	if _, err := db.ExecContext(ctx, schema); err != nil {
		db.Close()
		return nil, fmt.Errorf("sqlite: schema: %w", err)
	}
	if err := applyDurability(ctx, db, d); err != nil {
		db.Close()
		return nil, err
	}
	return &DB{db: db}, nil
}

func applyDurability(ctx context.Context, db *sql.DB, d Durability) error {
	var pragma string
	switch d {
	case DurabilityFull:
		pragma = "FULL"
	case DurabilityNormal, "":
		pragma = "NORMAL"
	case DurabilityOff:
		pragma = "OFF"
	default:
		return fmt.Errorf("sqlite: unknown durability %q", d)
	}
	_, err := db.ExecContext(ctx, "PRAGMA synchronous = "+pragma)
	return err
}

func (s *DB) Close() error { return s.db.Close() }

// DB exposes the handle for health checks.
func (s *DB) Ping(ctx context.Context) error { return s.db.PingContext(ctx) }

const insertTick = `
INSERT INTO ticks (
	venue, instrument, venue_trade_id, seq, event_ts_ns, recv_ts_ns,
	price_unscaled, price_scale, qty_unscaled, qty_scale, side, day
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
ON CONFLICT (venue, instrument, venue_trade_id) DO NOTHING`

// Append writes a batch in one transaction.
//
// One transaction per batch rather than per tick is the difference between
// roughly a thousand writes per second and roughly a hundred thousand: the
// fsync is per commit, not per row.
//
// Conflicts are ignored rather than updated. A tick is immutable — the same
// venue trade id always means the same trade — so a redelivery carries no
// new information, and treating it as an update would let a corrupted replay
// overwrite good history.
func (s *DB) Append(ctx context.Context, ticks []model.Tick) (int, error) {
	if len(ticks) == 0 {
		return 0, nil
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, fmt.Errorf("sqlite: begin: %w", err)
	}
	defer tx.Rollback()

	stmt, err := tx.PrepareContext(ctx, insertTick)
	if err != nil {
		return 0, fmt.Errorf("sqlite: prepare: %w", err)
	}
	defer stmt.Close()

	var stored int
	for _, t := range ticks {
		res, err := stmt.ExecContext(ctx,
			string(t.Venue), string(t.Instrument), t.VenueTradeID, int64(t.Seq),
			t.EventTime.UnixNano(), t.RecvTime.UnixNano(),
			t.Price.Unscaled, int64(t.Price.Scale),
			t.Quantity.Unscaled, int64(t.Quantity.Scale),
			int64(t.Side), day(t.EventTime),
		)
		if err != nil {
			return stored, fmt.Errorf("sqlite: insert %s/%s: %w", t.Venue, t.Instrument, err)
		}
		if n, err := res.RowsAffected(); err == nil {
			stored += int(n)
		}
	}

	if err := tx.Commit(); err != nil {
		return 0, fmt.Errorf("sqlite: commit: %w", err)
	}
	return stored, nil
}

// AppendRaw stores an undecoded frame.
func (s *DB) AppendRaw(ctx context.Context, venue model.VenueID, recv time.Time, payload []byte) error {
	_, err := s.db.ExecContext(ctx,
		`INSERT INTO raw_frames (venue, recv_ts_ns, day, payload) VALUES (?, ?, ?, ?)`,
		string(venue), recv.UnixNano(), day(recv), payload)
	if err != nil {
		return fmt.Errorf("sqlite: insert raw: %w", err)
	}
	return nil
}

// AppendRawBatch stores many frames in one transaction.
func (s *DB) AppendRawBatch(ctx context.Context, frames []RawFrame) error {
	if len(frames) == 0 {
		return nil
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("sqlite: begin raw: %w", err)
	}
	defer tx.Rollback()

	stmt, err := tx.PrepareContext(ctx,
		`INSERT INTO raw_frames (venue, recv_ts_ns, day, payload) VALUES (?, ?, ?, ?)`)
	if err != nil {
		return fmt.Errorf("sqlite: prepare raw: %w", err)
	}
	defer stmt.Close()

	for _, f := range frames {
		if _, err := stmt.ExecContext(ctx,
			string(f.Venue), f.RecvTime.UnixNano(), day(f.RecvTime), f.Payload); err != nil {
			return fmt.Errorf("sqlite: insert raw: %w", err)
		}
	}
	return tx.Commit()
}

// RawFrame is one undecoded exchange message.
type RawFrame struct {
	Venue    model.VenueID
	RecvTime time.Time
	Payload  []byte
}

// Ticks returns a page of the archive.
func (s *DB) Ticks(ctx context.Context, q store.Query) (store.Page, error) {
	limit := store.NormalizeLimit(q.Limit)

	var (
		where []string
		args  []any
	)
	if q.Venue != "" {
		where = append(where, "venue = ?")
		args = append(args, string(q.Venue))
	}
	if q.Instrument != "" {
		where = append(where, "instrument = ?")
		args = append(args, string(q.Instrument))
	}
	if !q.Start.IsZero() {
		where = append(where, "event_ts_ns >= ?")
		args = append(args, q.Start.UnixNano())
	}
	if !q.End.IsZero() {
		where = append(where, "event_ts_ns < ?")
		args = append(args, q.End.UnixNano())
	}

	// Keyset paging on (event_ts_ns, venue_trade_id). An OFFSET would skip
	// or repeat rows when ingest inserts into an earlier page mid-scan,
	// which on a live feed is constant.
	order := "ASC"
	cmp := ">"
	if q.Descending {
		order, cmp = "DESC", "<"
	}
	if !q.Cursor.IsZero() {
		where = append(where,
			fmt.Sprintf("(event_ts_ns %s ? OR (event_ts_ns = ? AND venue_trade_id %s ?))", cmp, cmp))
		args = append(args, q.Cursor.EventTimeNS, q.Cursor.EventTimeNS, q.Cursor.TradeID)
	}

	sb := strings.Builder{}
	sb.WriteString(`SELECT venue, instrument, venue_trade_id, seq, event_ts_ns, recv_ts_ns,
		price_unscaled, price_scale, qty_unscaled, qty_scale, side FROM ticks`)
	if len(where) > 0 {
		sb.WriteString(" WHERE " + strings.Join(where, " AND "))
	}
	fmt.Fprintf(&sb, " ORDER BY event_ts_ns %s, venue_trade_id %s LIMIT ?", order, order)
	// Fetch one extra row to learn whether another page exists without a
	// second COUNT query.
	args = append(args, limit+1)

	rows, err := s.db.QueryContext(ctx, sb.String(), args...)
	if err != nil {
		return store.Page{}, fmt.Errorf("sqlite: query ticks: %w", err)
	}
	defer rows.Close()

	page := store.Page{Ticks: make([]model.Tick, 0, limit)}
	for rows.Next() {
		t, err := scanTick(rows)
		if err != nil {
			return store.Page{}, err
		}
		page.Ticks = append(page.Ticks, t)
	}
	if err := rows.Err(); err != nil {
		return store.Page{}, fmt.Errorf("sqlite: scan ticks: %w", err)
	}

	if len(page.Ticks) > limit {
		page.Ticks = page.Ticks[:limit]
		page.HasMore = true
		last := page.Ticks[len(page.Ticks)-1]
		page.NextCursor = store.Cursor{
			EventTimeNS: last.EventTime.UnixNano(),
			TradeID:     last.VenueTradeID,
		}
	}
	return page, nil
}

// Latest returns the most recent tick for an instrument by event time.
func (s *DB) Latest(ctx context.Context, venue model.VenueID, id model.InstrumentID) (model.Tick, error) {
	row := s.db.QueryRowContext(ctx,
		`SELECT venue, instrument, venue_trade_id, seq, event_ts_ns, recv_ts_ns,
			price_unscaled, price_scale, qty_unscaled, qty_scale, side
		 FROM ticks WHERE venue = ? AND instrument = ?
		 ORDER BY event_ts_ns DESC, venue_trade_id DESC LIMIT 1`,
		string(venue), string(id))

	t, err := scanTick(row)
	if errors.Is(err, sql.ErrNoRows) {
		return model.Tick{}, store.ErrNotFound
	}
	return t, err
}

// Instruments summarizes the archive.
func (s *DB) Instruments(ctx context.Context) ([]store.InstrumentStat, error) {
	rows, err := s.db.QueryContext(ctx,
		`SELECT venue, instrument, COUNT(*), MIN(event_ts_ns), MAX(event_ts_ns)
		 FROM ticks GROUP BY venue, instrument ORDER BY venue, instrument`)
	if err != nil {
		return nil, fmt.Errorf("sqlite: query instruments: %w", err)
	}
	defer rows.Close()

	var out []store.InstrumentStat
	for rows.Next() {
		var (
			st              store.InstrumentStat
			venue, inst     string
			firstNS, lastNS int64
		)
		if err := rows.Scan(&venue, &inst, &st.Ticks, &firstNS, &lastNS); err != nil {
			return nil, fmt.Errorf("sqlite: scan instruments: %w", err)
		}
		st.Venue = model.VenueID(venue)
		st.Instrument = model.InstrumentID(inst)
		st.First = time.Unix(0, firstNS).UTC()
		st.Last = time.Unix(0, lastNS).UTC()
		out = append(out, st)
	}
	return out, rows.Err()
}

// scanner is satisfied by both *sql.Row and *sql.Rows.
type scanner interface{ Scan(dest ...any) error }

func scanTick(sc scanner) (model.Tick, error) {
	var (
		venue, inst, tradeID       string
		seq, eventNS, recvNS       int64
		priceU, priceS, qtyU, qtyS int64
		side                       int64
	)
	if err := sc.Scan(&venue, &inst, &tradeID, &seq, &eventNS, &recvNS,
		&priceU, &priceS, &qtyU, &qtyS, &side); err != nil {
		return model.Tick{}, err
	}
	return model.Tick{
		Venue:        model.VenueID(venue),
		Instrument:   model.InstrumentID(inst),
		Seq:          model.Seq(seq),
		EventTime:    time.Unix(0, eventNS).UTC(),
		RecvTime:     time.Unix(0, recvNS).UTC(),
		Price:        model.Decimal{Unscaled: priceU, Scale: uint8(priceS)},
		Quantity:     model.Decimal{Unscaled: qtyU, Scale: uint8(qtyS)},
		Side:         model.Side(side),
		VenueTradeID: tradeID,
	}, nil
}

// day is the partition key: the UTC date of the event.
func day(t time.Time) string { return t.UTC().Format("2006-01-02") }

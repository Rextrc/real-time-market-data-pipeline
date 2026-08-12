// Package httpapi serves the read side: current state, historical queries,
// health, metrics, and the paper account.
package httpapi

import (
	"context"
	_ "embed"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"strconv"
	"time"

	"github.com/Rextrc/real-time-market-data-pipeline/internal/bus"
	"github.com/Rextrc/real-time-market-data-pipeline/internal/consumer/candles"
	"github.com/Rextrc/real-time-market-data-pipeline/internal/consumer/paper"
	"github.com/Rextrc/real-time-market-data-pipeline/internal/consumer/persist"
	"github.com/Rextrc/real-time-market-data-pipeline/internal/model"
	"github.com/Rextrc/real-time-market-data-pipeline/internal/store"
)

// dashboardHTML is the operator dashboard, embedded into the binary so
// deploying the service is deploying the dashboard — no separate static
// hosting, no build step, no asset pipeline. It's plain HTML/CSS/JS against
// the JSON endpoints below; open internal/api/httpapi/static/dashboard.html
// to edit it directly and rebuild.
//
//go:embed static/dashboard.html
var dashboardHTML []byte

// Deps is everything the API reads from. All optional except Store — the API
// serves whatever is wired up and 404s the rest, so a partial deployment
// still starts.
type Deps struct {
	Store     store.Reader
	Bus       bus.Bus
	Candles   *candles.Builder
	Paper     []*paper.Engine
	Persister *persist.Persister
	// History serves recorded equity snapshots for the dashboard's equity
	// curve. Optional — a nil History just means that chart has nothing to
	// draw, everything else still works.
	History     store.EquityReader
	Venue       model.VenueID
	Instruments []model.InstrumentID
	StartedAt   time.Time
	Log         *slog.Logger
}

// Server exposes Deps over HTTP.
type Server struct {
	deps Deps
}

func New(d Deps) *Server { return &Server{deps: d} }

// Routes builds the mux. Kept separate from ListenAndServe so tests can
// exercise handlers without binding a port.
func (s *Server) Routes() http.Handler {
	mux := http.NewServeMux()

	// "/{$}" matches only the exact path "/" — not every unmatched subpath
	// under it — so this can't accidentally swallow a typo'd API route and
	// serve HTML where a client expected JSON.
	mux.HandleFunc("GET /{$}", s.dashboard)

	mux.HandleFunc("GET /healthz", s.health)
	mux.HandleFunc("GET /metrics", s.metrics)
	mux.HandleFunc("GET /v1/instruments", s.instruments)
	mux.HandleFunc("GET /v1/quote/{instrument}", s.quote)
	mux.HandleFunc("GET /v1/candles/{instrument}", s.candlesFor)
	mux.HandleFunc("GET /v1/ticks", s.ticks)
	mux.HandleFunc("GET /v1/paper", s.paperAll)
	mux.HandleFunc("GET /v1/paper/{strategy}", s.paperOne)
	mux.HandleFunc("GET /v1/paper/{strategy}/fills", s.paperFills)
	mux.HandleFunc("GET /v1/paper/{strategy}/history", s.paperHistory)

	return logging(s.deps.Log, mux)
}

// dashboard serves the operator UI. No-store: a redeploy should never leave
// a browser tab showing yesterday's dashboard behind a stale cache.
func (s *Server) dashboard(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.Header().Set("Cache-Control", "no-store")
	w.Write(dashboardHTML)
}

func (s *Server) health(w http.ResponseWriter, r *http.Request) {
	// Health is deliberately shallow: it answers "is this process serving",
	// not "is the whole pipeline healthy". A deep check that queries the
	// exchange would make a transient upstream blip look like a crash and
	// get the container restarted mid-run.
	writeJSON(w, http.StatusOK, map[string]any{
		"status":     "ok",
		"uptime_sec": int(time.Since(s.deps.StartedAt).Seconds()),
	})
}

// metrics reports queue depths, drops, and lag. This is the observability
// that makes backpressure visible instead of mysterious.
func (s *Server) metrics(w http.ResponseWriter, r *http.Request) {
	out := map[string]any{
		"uptime_sec": int(time.Since(s.deps.StartedAt).Seconds()),
	}
	if s.deps.Bus != nil {
		out["subscribers"] = s.deps.Bus.Stats()
	}
	if s.deps.Persister != nil {
		out["persist"] = s.deps.Persister.Stats()
	}
	if len(s.deps.Paper) > 0 {
		accounts := make(map[string]any, len(s.deps.Paper))
		for _, e := range s.deps.Paper {
			snap := e.Account().Snapshot(time.Now().UTC())
			accounts[e.Name()] = map[string]any{
				"equity":     snap.Equity,
				"total_pl":   snap.TotalPL,
				"return_pct": snap.ReturnPct,
				"trades":     snap.Trades,
			}
		}
		out["paper"] = accounts
	}

	// Archive size is the number that decides whether a long run survives on
	// a fixed-size volume, so it belongs next to the queue depths.
	if sizer, ok := s.deps.Store.(interface {
		SizeBytes(context.Context) (int64, error)
		Counts(context.Context) (int64, int64, error)
	}); ok {
		archive := map[string]any{}
		if size, err := sizer.SizeBytes(r.Context()); err == nil {
			archive["bytes"] = size
			archive["mb"] = size / (1 << 20)
		}
		if ticks, raw, err := sizer.Counts(r.Context()); err == nil {
			archive["ticks"] = ticks
			archive["raw_frames"] = raw
		}
		out["archive"] = archive
	}
	writeJSON(w, http.StatusOK, out)
}

func (s *Server) instruments(w http.ResponseWriter, r *http.Request) {
	stats, err := s.deps.Store.Instruments(r.Context())
	if err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"configured": s.deps.Instruments,
		"stored":     stats,
	})
}

func (s *Server) quote(w http.ResponseWriter, r *http.Request) {
	id := model.InstrumentID(r.PathValue("instrument"))

	tick, err := s.deps.Store.Latest(r.Context(), s.deps.Venue, id)
	if errors.Is(err, store.ErrNotFound) {
		writeError(w, http.StatusNotFound, err)
		return
	}
	if err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"venue":      tick.Venue,
		"instrument": tick.Instrument,
		"price":      tick.Price,
		"quantity":   tick.Quantity,
		"side":       tick.Side.String(),
		"event_time": tick.EventTime,
		"recv_time":  tick.RecvTime,
		"seq":        tick.Seq,
	})
}

func (s *Server) candlesFor(w http.ResponseWriter, r *http.Request) {
	if s.deps.Candles == nil {
		writeError(w, http.StatusNotFound, errors.New("candles not enabled"))
		return
	}

	id := model.InstrumentID(r.PathValue("instrument"))
	limit := intParam(r, "limit", 100)

	writeJSON(w, http.StatusOK, map[string]any{
		"instrument": id,
		"interval":   s.deps.Candles.Interval().String(),
		"candles":    s.deps.Candles.Recent(s.deps.Venue, id, limit),
	})
}

func (s *Server) ticks(w http.ResponseWriter, r *http.Request) {
	q := store.Query{
		Venue:      s.deps.Venue,
		Instrument: model.InstrumentID(r.URL.Query().Get("instrument")),
		Limit:      intParam(r, "limit", store.DefaultLimit),
		Descending: r.URL.Query().Get("order") == "desc",
	}

	if v := r.URL.Query().Get("start"); v != "" {
		t, err := time.Parse(time.RFC3339, v)
		if err != nil {
			writeError(w, http.StatusBadRequest, errors.New("start must be RFC3339"))
			return
		}
		q.Start = t
	}
	if v := r.URL.Query().Get("end"); v != "" {
		t, err := time.Parse(time.RFC3339, v)
		if err != nil {
			writeError(w, http.StatusBadRequest, errors.New("end must be RFC3339"))
			return
		}
		q.End = t
	}
	if v := r.URL.Query().Get("cursor_ts"); v != "" {
		ns, err := strconv.ParseInt(v, 10, 64)
		if err != nil {
			writeError(w, http.StatusBadRequest, errors.New("cursor_ts must be an integer"))
			return
		}
		q.Cursor = store.Cursor{EventTimeNS: ns, TradeID: r.URL.Query().Get("cursor_id")}
	}

	page, err := s.deps.Store.Ticks(r.Context(), q)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}

	resp := map[string]any{
		"ticks":    page.Ticks,
		"has_more": page.HasMore,
	}
	if page.HasMore {
		resp["next_cursor_ts"] = page.NextCursor.EventTimeNS
		resp["next_cursor_id"] = page.NextCursor.TradeID
	}
	writeJSON(w, http.StatusOK, resp)
}

// paperAll is the scoreboard: every strategy side by side on identical data.
func (s *Server) paperAll(w http.ResponseWriter, r *http.Request) {
	if len(s.deps.Paper) == 0 {
		writeError(w, http.StatusNotFound, errors.New("paper trading not enabled"))
		return
	}

	now := time.Now().UTC()
	results := make([]map[string]any, 0, len(s.deps.Paper))
	for _, e := range s.deps.Paper {
		results = append(results, map[string]any{
			"name":     e.Name(),
			"strategy": e.StrategyName(),
			"account":  e.Account().Snapshot(now),
		})
	}
	writeJSON(w, http.StatusOK, map[string]any{"strategies": results})
}

func (s *Server) paperOne(w http.ResponseWriter, r *http.Request) {
	e := s.engine(r.PathValue("strategy"))
	if e == nil {
		writeError(w, http.StatusNotFound, errors.New("no such strategy"))
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"name":     e.Name(),
		"strategy": e.StrategyName(),
		"account":  e.Account().Snapshot(time.Now().UTC()),
	})
}

func (s *Server) paperFills(w http.ResponseWriter, r *http.Request) {
	e := s.engine(r.PathValue("strategy"))
	if e == nil {
		writeError(w, http.StatusNotFound, errors.New("no such strategy"))
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"name":  e.Name(),
		"fills": e.Account().Fills(intParam(r, "limit", 100)),
	})
}

// paperHistory serves the equity curve for one strategy. It checks the
// strategy against the running engines (not just against whatever rows
// happen to exist) so a typo'd name 404s the same way paperOne/paperFills
// do, rather than returning an empty-but-200 result that looks like "this
// strategy has no history yet."
func (s *Server) paperHistory(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("strategy")
	if s.engine(name) == nil {
		writeError(w, http.StatusNotFound, errors.New("no such strategy"))
		return
	}
	if s.deps.History == nil {
		writeError(w, http.StatusNotFound, errors.New("equity history not enabled"))
		return
	}

	var since time.Time
	if v := r.URL.Query().Get("since"); v != "" {
		t, err := time.Parse(time.RFC3339, v)
		if err != nil {
			writeError(w, http.StatusBadRequest, errors.New("since must be RFC3339"))
			return
		}
		since = t
	}

	points, err := s.deps.History.EquityHistory(r.Context(), name, since, intParam(r, "limit", 0))
	if err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"name":   name,
		"points": points,
	})
}

func (s *Server) engine(name string) *paper.Engine {
	for _, e := range s.deps.Paper {
		if e.Name() == name {
			return e
		}
	}
	return nil
}

// Serve runs the HTTP server and shuts it down cleanly on context cancel.
func (s *Server) Serve(ctx context.Context, addr string) error {
	srv := &http.Server{
		Addr:              addr,
		Handler:           s.Routes(),
		ReadHeaderTimeout: 10 * time.Second,
		// No WriteTimeout: the websocket handler hijacks the connection and
		// manages its own deadlines. A blanket write timeout here would kill
		// long-lived streams.
		IdleTimeout: 120 * time.Second,
	}

	errc := make(chan error, 1)
	go func() {
		s.deps.Log.Info("http listening", "addr", addr)
		errc <- srv.ListenAndServe()
	}()

	select {
	case err := <-errc:
		if errors.Is(err, http.ErrServerClosed) {
			return nil
		}
		return err
	case <-ctx.Done():
		shutdownCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
		defer cancel()
		return srv.Shutdown(shutdownCtx)
	}
}

// Handle registers an extra route (used to mount the websocket endpoint).
func (s *Server) Handle(mux *http.ServeMux, pattern string, h http.Handler) {
	mux.Handle(pattern, h)
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}

func writeError(w http.ResponseWriter, code int, err error) {
	writeJSON(w, code, map[string]string{"error": err.Error()})
}

func intParam(r *http.Request, name string, def int) int {
	v := r.URL.Query().Get(name)
	if v == "" {
		return def
	}
	n, err := strconv.Atoi(v)
	if err != nil || n < 0 {
		return def
	}
	return n
}

func logging(log *slog.Logger, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		next.ServeHTTP(w, r)
		log.Debug("http", "method", r.Method, "path", r.URL.Path, "dur", time.Since(start))
	})
}

// Package wsapi pushes live ticks to browser and CLI clients.
//
// This is the egress side of the backpressure problem, and it is the same
// problem as ingest with the roles reversed: a client that stops reading is
// exactly as dangerous as a consumer that falls behind. A naive
// implementation buffers per-client writes until the process runs out of
// memory. This one gives every client a conflating queue, a write deadline,
// and a kick policy.
package wsapi

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"github.com/coder/websocket"

	"github.com/Rextrc/real-time-market-data-pipeline/internal/bus"
	"github.com/Rextrc/real-time-market-data-pipeline/internal/model"
)

const (
	// clientQueue is per-connection. It is small on purpose: with
	// conflation, depth beyond a few multiples of the instrument count
	// means the client is not keeping up, and buffering more only adds
	// staleness.
	clientQueue = 64

	// writeTimeout bounds a single frame write. Without it, one wedged
	// client blocks a goroutine forever.
	writeTimeout = 5 * time.Second

	// pingInterval detects clients that vanished without closing.
	pingInterval = 30 * time.Second
)

// Server fans live ticks out to websocket clients.
type Server struct {
	bus bus.Bus
	log *slog.Logger

	mu      sync.RWMutex
	clients map[*client]struct{}

	connected atomic.Int64
	kicked    atomic.Uint64
}

func New(b bus.Bus, log *slog.Logger) *Server {
	return &Server{bus: b, log: log, clients: make(map[*client]struct{})}
}

type client struct {
	conn *websocket.Conn
	out  chan model.Tick
	done chan struct{}
	once sync.Once
	// filter is empty for "everything".
	filter map[model.InstrumentID]struct{}
}

func (c *client) close() { c.once.Do(func() { close(c.done) }) }

type outbound struct {
	Venue      model.VenueID      `json:"venue"`
	Instrument model.InstrumentID `json:"instrument"`
	Seq        model.Seq          `json:"seq"`
	Price      model.Decimal      `json:"price"`
	Quantity   model.Decimal      `json:"quantity"`
	Side       string             `json:"side"`
	EventTime  time.Time          `json:"event_time"`
}

// Handler upgrades a request and streams ticks until the client goes away.
//
// Query parameter: ?instruments=BTC-USDT,ETH-USDT (omit for all).
func (s *Server) Handler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := websocket.Accept(w, r, &websocket.AcceptOptions{
			// Local dev and dashboards; tighten before exposing publicly.
			InsecureSkipVerify: true,
		})
		if err != nil {
			return
		}

		c := &client{
			conn: conn,
			out:  make(chan model.Tick, clientQueue),
			done: make(chan struct{}),
		}
		if raw := r.URL.Query().Get("instruments"); raw != "" {
			c.filter = parseFilter(raw)
		}

		s.add(c)
		s.connected.Add(1)
		defer func() {
			s.remove(c)
			s.connected.Add(-1)
			conn.CloseNow()
		}()

		s.serve(r.Context(), c)
	})
}

func (s *Server) serve(ctx context.Context, c *client) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	// A reader goroutine we ignore the content of. It exists so that close
	// frames and protocol errors are noticed promptly — without it, a
	// client that disconnects is only discovered on the next write.
	go func() {
		defer cancel()
		for {
			if _, _, err := c.conn.Read(ctx); err != nil {
				return
			}
		}
	}()

	ticker := time.NewTicker(pingInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-c.done:
			return

		case tick := <-c.out:
			payload, err := json.Marshal(outbound{
				Venue: tick.Venue, Instrument: tick.Instrument, Seq: tick.Seq,
				Price: tick.Price, Quantity: tick.Quantity,
				Side: tick.Side.String(), EventTime: tick.EventTime,
			})
			if err != nil {
				continue
			}

			// The write deadline is what turns "this client wedged" into a
			// bounded, recoverable event instead of a leaked goroutine.
			wctx, wcancel := context.WithTimeout(ctx, writeTimeout)
			err = c.conn.Write(wctx, websocket.MessageText, payload)
			wcancel()
			if err != nil {
				s.log.Debug("ws write failed, dropping client", "err", err)
				return
			}

		case <-ticker.C:
			pctx, pcancel := context.WithTimeout(ctx, writeTimeout)
			err := c.conn.Ping(pctx)
			pcancel()
			if err != nil {
				return
			}
		}
	}
}

// Run subscribes to the bus and fans ticks into per-client queues.
//
// The bus subscription itself uses PolicyCoalesce, so the websocket fan-out
// can never apply backpressure to ingest no matter how many slow clients are
// attached. Per-client queues then use drop-oldest: a live price feed wants
// the newest tick, and a client too slow to keep up is better served stale-
// free-but-gappy than lagging further behind every second.
func (s *Server) Run(ctx context.Context) error {
	sub, err := s.bus.Subscribe(bus.SubscriberSpec{
		Name:     "websocket-fanout",
		Capacity: 2048,
		Policy:   bus.PolicyCoalesce,
	})
	if err != nil {
		return err
	}
	defer sub.Close()

	for {
		select {
		case <-ctx.Done():
			return nil
		case <-sub.Done():
			return nil
		case tick := <-sub.Ticks():
			s.broadcast(tick)
		}
	}
}

func (s *Server) broadcast(t model.Tick) {
	s.mu.RLock()
	targets := make([]*client, 0, len(s.clients))
	for c := range s.clients {
		targets = append(targets, c)
	}
	s.mu.RUnlock()

	for _, c := range targets {
		if c.filter != nil {
			if _, ok := c.filter[t.Instrument]; !ok {
				continue
			}
		}

		select {
		case c.out <- t:
			continue
		default:
		}

		// Queue full: evict the oldest and retry once. If it is still full
		// the client is pathologically slow — kick it rather than let it
		// degrade the server.
		select {
		case <-c.out:
		default:
		}
		select {
		case c.out <- t:
		default:
			s.kicked.Add(1)
			s.log.Warn("kicking unresponsive websocket client")
			c.close()
		}
	}
}

// Stats reports connection counts for /metrics.
func (s *Server) Stats() map[string]any {
	return map[string]any{
		"connected": s.connected.Load(),
		"kicked":    s.kicked.Load(),
	}
}

func (s *Server) add(c *client) {
	s.mu.Lock()
	s.clients[c] = struct{}{}
	s.mu.Unlock()
}

func (s *Server) remove(c *client) {
	s.mu.Lock()
	delete(s.clients, c)
	s.mu.Unlock()
	c.close()
}

func parseFilter(raw string) map[model.InstrumentID]struct{} {
	out := make(map[model.InstrumentID]struct{})
	start := 0
	for i := 0; i <= len(raw); i++ {
		if i == len(raw) || raw[i] == ',' {
			if s := raw[start:i]; s != "" {
				out[model.InstrumentID(s)] = struct{}{}
			}
			start = i + 1
		}
	}
	if len(out) == 0 {
		return nil
	}
	return out
}
